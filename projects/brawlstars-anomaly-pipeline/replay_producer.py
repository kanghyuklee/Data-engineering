"""
replay_producer.py
------------------
저장된 배틀로그 JSON들을 읽어서 '한 배틀 = 한 이벤트'로 만들어 Kafka로 재생한다.
battleTime 순서로 정렬해 내보내고, --rate로 초당 전송 건수를 조절한다. (6회차 로드 테스트 핵심)

Producer 흐름:  읽기 → 공통 껍데기로 감싸 직렬화 → 전송 → 콜백으로 성공/실패 확인

실행 예:
  python replay_producer.py --in data/raw --rate 300        # 초당 300건
  python replay_producer.py --in data/raw --rate 30000      # 부하 테스트
  python replay_producer.py --in data/raw --rate 0          # 최대 속도(대기 없음)
"""

import argparse
import glob
import hashlib
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BATTLES_TOPIC = "bs.battles"
SCHEMA_VERSION = 1

# 전송 결과 집계 (콜백에서 갱신)
_stats = {"ok": 0, "fail": 0}


def make_producer(bootstrap: str) -> KafkaProducer:
    """안정성 위주 설정으로 Producer 생성."""
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode() if k else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
        acks="all",                 # 모든 복제본이 받아야 성공 (유실 방지)
        enable_idempotence=True,    # 재시도해도 중복 저장 안 됨 (정확히 한 번에 근접)
        compression_type="lz4",     # 네트워크·디스크 절약, 처리량↑
        retries=5,
        linger_ms=10,               # 모아서 전송 (6회차에 batch_size와 함께 튜닝)
    )


def on_success(_md):
    _stats["ok"] += 1


def on_error(exc):
    _stats["fail"] += 1
    print(f"  [전송 실패] {exc}")   # 실무라면 여기서 DLQ로 보내거나 재시도


def parse_battle_time(bt: str) -> datetime:
    # 브롤스타즈 포맷: 20260115T120000.000Z
    return datetime.strptime(bt, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=timezone.utc)


def battle_id(bt: str, tags: list[str]) -> str:
    raw = bt + "|" + "|".join(sorted(tags))
    return hashlib.md5(raw.encode()).hexdigest()[:10]


def to_payload(owner_tag: str, b: dict) -> dict | None:
    """배틀로그 한 건을 payload(실제 데이터)로 변환 (로그 주인 owner_tag 관점)."""
    battle = b.get("battle", {})
    event = b.get("event", {})
    teams = battle.get("teams") or []

    # 팀 모드만 우선 처리. TODO: 쇼다운(players 키) 분기 / 팀원-상대 구분
    all_tags, me = [], None
    for team in teams:
        for p in team:
            all_tags.append(p["tag"])
            if p["tag"] == owner_tag:
                me = p
    if me is None:
        return None

    opponents = [t for t in all_tags if t != owner_tag]
    bt = b["battleTime"]
    brawler = me.get("brawler", {})
    star = battle.get("starPlayer") or {}

    return {
        "battle_id": battle_id(bt, all_tags),
        "player_tag": owner_tag,
        "mode": battle.get("mode") or event.get("mode"),
        "map": event.get("map"),
        "type": battle.get("type"),
        "result": battle.get("result"),          # victory/defeat/draw
        "duration_s": battle.get("duration"),
        "trophy_change": battle.get("trophyChange"),
        "is_star_player": star.get("tag") == owner_tag,
        "brawler_id": brawler.get("id"),
        "brawler_name": brawler.get("name"),
        "brawler_power": brawler.get("power"),
        "brawler_trophies": brawler.get("trophies"),
        "opponents": opponents,
        "is_injected": int(b.get("_injected", 0)),
    }


def to_envelope(owner_tag: str, b: dict) -> dict | None:
    """payload를 공통 껍데기로 감싼다. 모든 이벤트가 같은 바깥 구조를 갖는다."""
    payload = to_payload(owner_tag, b)
    if payload is None:
        return None
    bt = b["battleTime"]
    return {
        "event_id": f"{bt}_{owner_tag}",            # 중복 제거·멱등 저장 기준
        "event_type": "battle",                     # 이벤트 종류
        "event_time": parse_battle_time(bt).isoformat(),  # 발생 시각
        "schema_version": SCHEMA_VERSION,           # 필드 바뀌어도 consumer 안 깨짐
        "source": "replay-generator",               # 출처
        "payload": payload,                         # 종류별 실제 데이터
    }


def load_events(in_dir: str) -> list[dict]:
    events = []
    for path in glob.glob(os.path.join(in_dir, "*.json")):
        owner_tag = "#" + os.path.splitext(os.path.basename(path))[0]
        with open(path, encoding="utf-8") as f:
            battles = json.load(f)
        for b in battles:
            env = to_envelope(owner_tag, b)
            if env:
                events.append(env)
    events.sort(key=lambda e: e["event_time"])   # 발생 시각 순 재생
    return events


def run(in_dir: str, bootstrap: str, rate: float):
    producer = make_producer(bootstrap)
    events = load_events(in_dir)
    interval = 1.0 / rate if rate > 0 else 0.0

    for env in events:
        key = env["payload"]["player_tag"]          # key=계정 → 같은 계정 순서 보장
        (producer.send(BATTLES_TOPIC, key=key, value=env)
         .add_callback(on_success)
         .add_errback(on_error))                    # 전송 결과를 콜백으로 확인
        if interval:
            time.sleep(interval)

    producer.flush()                                # 남은 메시지까지 모두 전송 보장
    producer.close()
    print(f"done. 전송 성공 {_stats['ok']} / 실패 {_stats['fail']} (총 {len(events)})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_dir", default="data/raw")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--rate", type=float, default=300,
                   help="초당 전송 건수 (0이면 최대 속도). 6회차 로드 테스트에서 올림")
    args = p.parse_args()
    run(args.in_dir, args.bootstrap, args.rate)


if __name__ == "__main__":
    main()

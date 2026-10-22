"""
replay_producer.py
------------------
저장된 배틀로그 JSON들을 읽어서 '한 배틀 = 한 이벤트'로 만들어 Kafka로 재생한다.
battleTime 순서로 정렬해 내보내고, --speed로 배속을 조절한다. (6회차 로드 테스트 핵심)

실행 예:
  python replay_producer.py --in data/raw --speed 1.0
  python replay_producer.py --in data/raw --speed 50     # 부하 테스트
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


def make_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode() if k else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
        acks="all",   # 유실 방지
    )


def parse_battle_time(bt: str) -> datetime:
    # 브롤스타즈 포맷: 20260115T120000.000Z
    return datetime.strptime(bt, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=timezone.utc)


def battle_id(bt: str, tags: list[str]) -> str:
    raw = bt + "|" + "|".join(sorted(tags))
    return hashlib.md5(raw.encode()).hexdigest()[:10]


def to_event(owner_tag: str, b: dict) -> dict | None:
    """배틀로그 한 건을 bs.battles 이벤트로 변환 (로그 주인 owner_tag 관점)."""
    battle = b.get("battle", {})
    event = b.get("event", {})
    teams = battle.get("teams") or []

    # 팀 모드만 우선 처리. TODO: 쇼다운(players 키) 모드 분기
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
        "event_id": f"{bt}_{owner_tag}",
        "battle_id": battle_id(bt, all_tags),
        "player_tag": owner_tag,
        "ts": parse_battle_time(bt).isoformat(),
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
        "is_injected": int(b.get("_injected", 0)),  # anomaly_injector가 심는 플래그
    }


def load_events(in_dir: str) -> list[dict]:
    events = []
    for path in glob.glob(os.path.join(in_dir, "*.json")):
        owner_tag = "#" + os.path.splitext(os.path.basename(path))[0]
        with open(path, encoding="utf-8") as f:
            battles = json.load(f)
        for b in battles:
            ev = to_event(owner_tag, b)
            if ev:
                events.append(ev)
    events.sort(key=lambda e: e["ts"])  # battleTime 순 재생
    return events


def run(in_dir: str, bootstrap: str, speed: float, base_gap: float):
    producer = make_producer(bootstrap)
    events = load_events(in_dir)
    for ev in events:
        producer.send(BATTLES_TOPIC, key=ev["player_tag"], value=ev)  # key=계정 → 순서 보장
        time.sleep(base_gap / speed)
    producer.flush()
    print(f"done. sent {len(events)} battle events")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_dir", default="data/raw")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--speed", type=float, default=1.0, help="재생 배속 (클수록 빠름)")
    p.add_argument("--base-gap", type=float, default=0.1, help="배틀 사이 기본 간격(초)")
    args = p.parse_args()
    run(args.in_dir, args.bootstrap, args.speed, args.base_gap)


if __name__ == "__main__":
    main()

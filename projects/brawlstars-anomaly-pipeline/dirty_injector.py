"""
dirty_injector.py — 전처리 검증용: 일부러 '지저분한' 메시지를 Kafka에 넣는다.
----------------------------------------------------------------------
목적: preprocess.py의 전처리(파싱·결측·중복 제거)가 실제로 동작하는지 확인.
      이 스크립트로 더러운 메시지를 넣은 뒤 preprocess를 돌리면
      "[입력] N건 → [정제] M건" 에서 N > M 이 되어야 한다(= 걸러졌다는 증거).

넣는 4가지 유형 (각각 어느 전처리 단계가 잡는지):
  1) 깨진 JSON        → from_json 실패 → event_id=null → dropna 가 제거
  2) player_tag 없음  → payload.player_tag=null → dropna 가 제거
  3) event_time 이상  → to_timestamp=null → dropna 가 제거
  4) 중복 event_id    → 같은 id n건 → dropDuplicates 가 1건만 남김

실행 (호스트에서. Kafka가 localhost:9092로 떠 있어야 함):
  PYTHONUTF8=1 python dirty_injector.py            # 종류별 2건씩
  PYTHONUTF8=1 python dirty_injector.py --n 3      # 종류별 3건씩
"""

import argparse
import json

from kafka import KafkaProducer

BOOTSTRAP = "localhost:9092"   # 호스트에서 실행 → 호스트 리스너
TOPIC = "bs.battles"


def base_envelope(event_id: str) -> dict:
    """producer.py와 같은 공통 껍데기 + 최소한의 정상 payload."""
    return {
        "event_id": event_id,
        "event_type": "battle",
        "event_time": "2026-10-21T20:56:00.000Z",
        "schema_version": 1,
        "source": "dirty_injector",
        "payload": {
            "battle_id": event_id,
            "player_tag": "#DIRTYTEST",
            "mode": "gemGrab",
            "map": "Hard Rock Mine",
            "type": "ranked",
            "result": "victory",
            "duration_s": 90,
            "trophy_change": 8,
            "is_star_player": False,
            "brawler_name": "SHELLY",
            "brawler_trophies": 500,
            "teammates": ["#AAA", "#BBB"],
            "opponents": ["#CCC", "#DDD", "#EEE"],
            "is_injected": 1,
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--n", type=int, default=2, help="유형별 건수 (기본 2)")
    args = ap.parse_args()

    p = KafkaProducer(bootstrap_servers=args.bootstrap)
    n = args.n
    sent = 0

    # 1) 깨진 JSON — 유효한 JSON이 아님 (파싱 자체가 실패)
    for _ in range(n):
        p.send(TOPIC, key=b"dirty", value=b'{"event_id": "broken", not valid json here ')
        sent += 1

    # 2) player_tag 없음 — 껍데기는 맞지만 필수 필드가 null (누구 배틀인지 모름)
    for i in range(n):
        msg = base_envelope(f"nulltag_{i}")
        msg["payload"]["player_tag"] = None
        p.send(TOPIC, key=b"dirty", value=json.dumps(msg).encode())
        sent += 1

    # 3) event_time 이상 — timestamp로 변환 불가한 문자열 (언제인지 모름)
    for i in range(n):
        msg = base_envelope(f"badtime_{i}")
        msg["event_time"] = "언제인지모름"
        p.send(TOPIC, key=b"dirty", value=json.dumps(msg).encode())
        sent += 1

    # 4) 중복 event_id — 똑같은 id로 n건 (재전송/재실행으로 생기는 중복 재현)
    for _ in range(n):
        msg = base_envelope("DUP_DEMO")   # event_id 고정 → 전부 중복
        p.send(TOPIC, key=b"dirty", value=json.dumps(msg).encode())
        sent += 1

    p.flush()
    p.close()

    removed = n + n + n + (n - 1)   # 깨진+결측+이상 전부 제거, 중복은 1건만 남음
    print(f"지저분한 메시지 {sent}건 전송 완료 (유형 4개 × {n}건)")
    print(f"  - 깨진 JSON {n} · player_tag 결측 {n} · event_time 이상 {n} · 중복 {n}(→1건만 생존)")
    print(f"예상: 이 중 {removed}건이 전처리에서 제거됨 (중복 생존 1건은 정상 신규로 남음)")


if __name__ == "__main__":
    main()

"""
consumer.py
-----------
Kafka(bs.battles)를 소비해서 원본 배틀 이벤트를 Parquet으로 적재하는 컨슈머.
Spark 탐지 경로와는 '별도 컨슈머 그룹'으로 돈다 (원본 보존 + 재처리용).

실행 예:
  python consumer.py --bootstrap localhost:9092 --out data/parquet
"""

import argparse
import json

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

BATTLES_TOPIC = "bs.battles"
BATCH_SIZE = 10_000   # 이 개수만큼 모이면 Parquet 파일 하나로 flush


def run(bootstrap: str, out_dir: str):
    consumer = KafkaConsumer(
        BATTLES_TOPIC,
        bootstrap_servers=bootstrap,
        group_id="raw-archiver",          # Spark와 다른 그룹
        auto_offset_reset="earliest",
        enable_auto_commit=False,         # 적재 성공 후 수동 커밋 (유실 방지)
        value_deserializer=lambda b: json.loads(b.decode()),
    )

    buffer, part = [], 0
    for msg in consumer:
        buffer.append(msg.value)
        if len(buffer) >= BATCH_SIZE:
            _flush(buffer, out_dir, part)
            consumer.commit()
            buffer.clear()
            part += 1
    # TODO: 종료 시그널(SIGINT) 받으면 남은 buffer flush


def _flush(rows: list, out_dir: str, part: int):
    # opponents(list) 컬럼 때문에 스키마가 흔들릴 수 있으니 JSON 문자열로 평탄화
    for r in rows:
        if isinstance(r.get("opponents"), list):
            r["opponents"] = ",".join(r["opponents"])
    table = pa.Table.from_pylist(rows)
    path = f"{out_dir}/battles_part_{part:05d}.parquet"
    pq.write_table(table, path)
    print(f"wrote {len(rows)} rows -> {path}")
    # TODO: 날짜 기준 파티셔닝(out_dir/dt=YYYY-MM-DD/...)으로 바꾸면 재처리가 쉬움


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--out", default="data/parquet")
    args = p.parse_args()
    run(args.bootstrap, args.out)


if __name__ == "__main__":
    main()

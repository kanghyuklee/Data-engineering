"""
preprocess.py — 4회차: Spark 전처리 · 저장
----------------------------------------
입력: Kafka 토픽 bs.battles (수집 구간이 넣어둔 이벤트)   ← Kafka를 실제 소스로 사용
변환: 전처리(자료형·시각·결측·중복) → 계정별 집계 → 이상 스코어
저장: Parquet(정제 원본, 재처리용) + PostgreSQL(탐지 결과, 조회용)

Kafka에서 '배치로' 읽는다(earliest~latest 전체). 재실행하면 전체를 다시 읽어
결과를 덮어쓰므로(overwrite) 멱등하다.

실행 (Spark 컨테이너 안에서 — 로컬 Java/pyspark 설치 불필요):
  docker compose up -d kafka postgres spark
  docker compose exec spark spark-submit --master "local[*]" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    /app/preprocess.py
"""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, BooleanType, ArrayType)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")   # 컨테이너 내부 리스너
TOPIC = "bs.battles"
PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/brawlstars")
PG_PROPS = {"user": "brawl", "password": "brawl", "driver": "org.postgresql.Driver"}
OUT_PARQUET = os.environ.get("OUT_PARQUET", "/app/data/parquet/battles")

# producer가 보낸 공통 껍데기 + payload 구조
PAYLOAD = StructType([
    StructField("battle_id", StringType()),
    StructField("player_tag", StringType()),
    StructField("mode", StringType()),
    StructField("map", StringType()),
    StructField("type", StringType()),
    StructField("result", StringType()),
    StructField("duration_s", IntegerType()),
    StructField("trophy_change", IntegerType()),
    StructField("is_star_player", BooleanType()),
    StructField("brawler_name", StringType()),
    StructField("brawler_trophies", IntegerType()),
    StructField("teammates", ArrayType(StringType())),
    StructField("opponents", ArrayType(StringType())),
    StructField("is_injected", IntegerType()),
])
ENVELOPE = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("payload", PAYLOAD),
])


def main():
    spark = SparkSession.builder.appName("bs-preprocess").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1) Kafka에서 배치로 읽기 (전체 범위) ──────────────────────────
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    total_in = raw.count()

    # ── 2) 전처리 ────────────────────────────────────────────────
    parsed = (raw
              .select(F.from_json(F.col("value").cast("string"), ENVELOPE).alias("e"))
              .select("e.event_id", "e.event_time", "e.payload.*"))

    df = (parsed
          .withColumn("event_time", F.to_timestamp("event_time"))   # 자료형: 문자열 → timestamp(UTC, 발생 시각)
          .dropna(subset=["event_id", "player_tag", "event_time"])  # 결측: 필수 필드 없는 행 제거
          .dropDuplicates(["event_id"]))                            # 중복: event_id 기준 제거
    clean_cnt = df.count()

    # ── 3) 정제 원본 → Parquet (날짜 파티셔닝, 재처리용) ──────────────
    (df.withColumn("dt", F.to_date("event_time"))
       .write.mode("overwrite").partitionBy("dt").parquet(OUT_PARQUET))

    # ── 4) 계정별 집계 → 이상 스코어 ─────────────────────────────
    is_win = F.when(F.col("result") == "victory", 1.0).otherwise(0.0)
    agg = (df.groupBy("player_tag").agg(
        F.count("*").alias("battles"),
        F.round(F.avg(is_win), 3).alias("win_rate"),
        F.sum("trophy_change").alias("trophy_gain"),
        F.countDistinct("brawler_name").alias("brawler_variety"),
        F.max("is_injected").alias("is_injected"),
    ))
    scored = (agg
        # 룰 기반 이상 스코어 (임계값은 데이터 보며 조정)
        .withColumn("anomaly_score",
            F.when((F.col("win_rate") > 0.9) & (F.col("battles") >= 15), F.lit(0.8))
             .when(F.col("brawler_variety") <= 1, F.lit(0.6))
             .otherwise(F.lit(0.1)))
        .withColumn("anomaly_reason",
            F.when(F.col("brawler_variety") <= 1, F.lit("bot"))
             .when(F.col("win_rate") > 0.9, F.lit("boosting"))
             .otherwise(F.lit("normal")))
        .withColumn("updated_at", F.current_timestamp())
        # schema.sql의 detection_result 컬럼 순서에 맞춤
        .select("player_tag", "battles", "win_rate", "trophy_gain",
                "brawler_variety", "anomaly_score", "anomaly_reason",
                "is_injected", "updated_at"))

    # ── 5) PostgreSQL 저장 (결과 스냅샷, overwrite = 재실행해도 멱등) ──
    (scored.write.mode("overwrite").jdbc(PG_URL, "detection_result", properties=PG_PROPS))

    # ── 입출력 예시 출력 ──────────────────────────────────────────
    print(f"[입력] Kafka 메시지 {total_in}건  →  [정제] {clean_cnt}건 (중복·결측 제거 후)")
    print("[출력] 계정별 탐지 결과 상위 의심:")
    scored.orderBy(F.desc("anomaly_score")).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

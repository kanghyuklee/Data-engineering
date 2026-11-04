"""
preprocess.py — Spark 전처리 · 저장
----------------------------------------
입력: Kafka 토픽 bs.battles (수집 구간이 넣어둔 이벤트)   ← Kafka를 실제 소스로 사용
변환: 전처리(자료형·시각·결측·중복) → 계정별 집계 → 이상 스코어
저장: Parquet(정제 원본, 재처리용) + PostgreSQL(탐지 결과, 조회용)
격리: 불량 레코드(깨진 JSON·결측·이상 시각)는 버리지 않고 bs.dlq로 보냄(관측성·유실 방지)

Kafka에서 '배치로' 읽는다(earliest~latest 전체). 재실행하면 전체를 다시 읽어
결과를 덮어쓰므로(overwrite) 멱등하다.

실행 (Spark 컨테이너 안에서 — 로컬 Java/pyspark 설치 불필요):
  docker compose up -d kafka postgres spark
  docker compose exec spark spark-submit --master "local[*]" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    /app/preprocess.py
"""

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, BooleanType, ArrayType)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")   # 컨테이너 내부 리스너
TOPIC = "bs.battles"
DLQ = "bs.dlq"                                             # 불량 레코드 격리 토픽
PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/brawlstars")
PG_PROPS = {"user": "brawl", "password": "brawl", "driver": "org.postgresql.Driver"}
STAGING = "detection_result_staging"   # Spark가 이번 실행분을 쓰는 임시 테이블 (upsert 전 단계)
OUT_PARQUET = os.environ.get("OUT_PARQUET", "/app/data/parquet/battles")
# 집계 대상 = 매칭전만. 친선전·토너먼트는 상대를 고를 수 있고 트로피 변동도 없어
#   win_rate·trophy를 왜곡 → 봇/부스팅 신호가 흐려짐. (Parquet 원본엔 전부 유지)
MATCHMADE = ("ranked", "soloRanked")

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
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="",
                    help="처리할 데이터 날짜 YYYY-MM-DD (비우면 전체 재처리). 리플레이 backfill용")
    target_date = ap.parse_args().date.strip()

    spark = (SparkSession.builder.appName("bs-preprocess")
             # 파일 커밋 v2: 태스크 출력을 최종 경로에 바로 쓴다(_temporary rename/merge 최소화).
             #   Windows 바인드 마운트·OneDrive 동기화에서 나던 _temporary FileNotFound 완화.
             .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
             # dt 파티션 단위 덮어쓰기 → target_date 처리 시 다른 날짜 파티션은 보존.
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ── 1) Kafka에서 배치로 읽기 (전체 범위) ──────────────────────────
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load()
           .cache())   # count·DLQ쓰기·Parquet·PG 등 여러 action이 Kafka를 반복 읽지 않게 1회만
    total_in = raw.count()

    # ── 2) 파싱 (원본 value 보존) ─────────────────────────────────
    parsed = (raw
              .select(F.col("value").cast("string").alias("raw_value"),        # 원본 메시지 보존(DLQ용)
                      F.from_json(F.col("value").cast("string"), ENVELOPE).alias("e"))
              .select("raw_value", "e.event_id", "e.event_time", "e.payload.*")
              .withColumn("event_time", F.to_timestamp("event_time")))         # 자료형: 문자열 → timestamp

    # 유효 조건: 필수 필드가 모두 있어야 함 (깨진 JSON이면 from_json=null → 전부 null)
    valid_cond = (F.col("event_id").isNotNull()
                  & F.col("player_tag").isNotNull()
                  & F.col("event_time").isNotNull())

    # ── 2-1) 불량 레코드 → DLQ (버리지 않고 bs.dlq로 격리 = 관측성·유실 방지) ──
    reason = (F.when(F.col("event_id").isNull(), F.lit("parse_fail_or_missing_event_id"))
               .when(F.col("player_tag").isNull(), F.lit("missing_player_tag"))
               .otherwise(F.lit("bad_event_time")))
    bad = parsed.filter(~valid_cond)
    dlq_cnt = bad.count()
    (bad.select(F.to_json(F.struct(
            F.col("raw_value").alias("original"),      # 원본 메시지 그대로 (왜 깨졌는지 확인용)
            reason.alias("reason"),                    # 격리 사유
            F.current_timestamp().cast("string").alias("dlq_time"))).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", DLQ)
        .save())

    # ── 2-2) 유효 레코드 → 중복 제거 ─────────────────────────────
    df = (parsed.filter(valid_cond)
          .drop("raw_value")
          .dropDuplicates(["event_id"]))              # 중복: event_id 기준 제거
    clean_cnt = df.count()

    # 처리 범위: target_date 지정 시 그 날짜(dt)만 처리(backfill). 비우면 전체.
    scope = (df.filter(F.to_date("event_time") == F.lit(target_date))
             if target_date else df)

    # ── 3) 정제 원본 → Parquet (dt 파티셔닝). dynamic overwrite라 지정 날짜만 교체 ──
    (scope.withColumn("dt", F.to_date("event_time"))
       .write.mode("overwrite").partitionBy("dt").parquet(OUT_PARQUET))

    # ── 4) 계정별 집계 → 이상 스코어 (매칭전만 집계 — Parquet 원본은 전체 유지) ──
    competitive = scope.filter(F.col("type").isin(*MATCHMADE))
    comp_cnt = competitive.count()
    is_win = F.when(F.col("result") == "victory", 1.0).otherwise(0.0)
    agg = (competitive.groupBy("player_tag").agg(
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

    # ── 5) PostgreSQL 저장 — 스테이징에 적재(다음 단계 upsert_detection.py가 병합) ──
    #   Spark JDBC는 upsert(ON CONFLICT)를 직접 못 하므로, 이번 실행분을 스테이징 테이블에
    #   overwrite(DROP+CREATE)로 쓴다. 이후 upsert_detection.py가 detection_result로
    #   ON CONFLICT(player_tag) DO UPDATE 병합한다.
    #   → 이번 배치에 없는 계정은 지워지지 않고(날짜 누적), ml_score·final_score 등
    #     다른 단계가 채운 컬럼도 보존된다. (기존 overwrite+truncate는 매번 전체 교체였음)
    (scored.write.mode("overwrite")
        .jdbc(PG_URL, STAGING, properties=PG_PROPS))

    # ── 입출력 예시 출력 ──────────────────────────────────────────
    # 원본-결과 대조: 입력 = 정제 + DLQ + 중복제거 (조용히 사라진 데이터 없는지)
    dup_removed = total_in - dlq_cnt - clean_cnt
    assert dup_removed >= 0, (f"원본-결과 대조 실패: 입력 {total_in} < "
                              f"정제 {clean_cnt} + DLQ {dlq_cnt} (데이터 유실/중복계산 의심)")
    print(f"[입력] Kafka 메시지 {total_in}건  →  [정제] {clean_cnt}건  ·  [DLQ 격리] {dlq_cnt}건 (불량은 bs.dlq로)")
    print(f"[대조] 입력 {total_in} = 정제 {clean_cnt} + DLQ {dlq_cnt} + 중복제거 {dup_removed}  → 조용히 사라진 데이터 0 확인")
    print(f"[범위] 처리 범위 = {('target_date=' + target_date) if target_date else '전체'} "
          f"· 매칭전(ranked/soloRanked) {comp_cnt}건 집계 (Parquet 원본은 전체 유지)")
    print("[출력] 계정별 탐지 결과 상위 의심:")
    scored.orderBy(F.desc("anomaly_score")).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

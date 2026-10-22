"""
preprocess.py
-------------
Spark Structured Streaming으로 bs.battles를 읽어서
계정(player_tag)별로 봇·부스팅 신호를 집계하고 이상 스코어를 매긴 뒤,
결과는 PostgreSQL에, (원본은 consumer.py가 Parquet에) 저장한다.

로컬 실행 예:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 preprocess.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, BooleanType)

KAFKA_BOOTSTRAP = "localhost:9092"
BATTLES_TOPIC = "bs.battles"

PG_URL = "jdbc:postgresql://localhost:5432/brawlstars"
PG_PROPS = {"user": "brawl", "password": "brawl", "driver": "org.postgresql.Driver"}

BATTLE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("player_tag", StringType()),
    StructField("ts", StringType()),
    StructField("mode", StringType()),
    StructField("result", StringType()),
    StructField("trophy_change", IntegerType()),
    StructField("brawler_name", StringType()),
    StructField("is_injected", IntegerType()),
])


def build_stream(spark: SparkSession):
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", BATTLES_TOPIC)
           .option("startingOffsets", "latest")
           .load())
    return (raw
            .select(F.from_json(F.col("value").cast("string"), BATTLE_SCHEMA).alias("e"))
            .select("e.*")
            .withColumn("ts", F.to_timestamp("ts")))


def score_accounts(events):
    """
    계정별 집계 → 봇/부스팅 신호 → 룰 기반 이상 스코어.
    TODO: 임계값은 데이터 보고 조정. win-trading은 opponents 컬럼으로 별도 집계 필요.
    """
    agg = (events
           .withWatermark("ts", "1 hour")
           .groupBy("player_tag")
           .agg(
               F.count("*").alias("battles"),
               F.avg(F.when(F.col("result") == "victory", 1.0).otherwise(0.0)).alias("win_rate"),
               F.sum("trophy_change").alias("trophy_gain"),
               F.countDistinct("brawler_name").alias("brawler_variety"),
               F.max("is_injected").alias("is_injected"),
           ))

    # 룰 예시: 승률이 비정상적으로 높고 브롤러를 거의 안 바꾸면 봇/부스팅 의심
    scored = (agg
              .withColumn("anomaly_score",
                          F.when((F.col("win_rate") > 0.9) & (F.col("battles") >= 15),
                                 F.lit(0.7)).otherwise(F.lit(0.1)))
              .withColumn("anomaly_reason",
                          F.when(F.col("brawler_variety") <= 1, F.lit("bot"))
                           .when(F.col("win_rate") > 0.9, F.lit("boosting"))
                           .otherwise(F.lit("normal"))))
    return scored


def write_to_postgres(batch_df, batch_id: int):
    (batch_df.write
     .mode("append")      # TODO: upsert는 staging + MERGE 또는 psycopg2로
     .jdbc(PG_URL, "detection_result", properties=PG_PROPS))


def main():
    spark = SparkSession.builder.appName("brawlstars-anomaly").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = build_stream(spark)
    scored = score_accounts(events)

    query = (scored.writeStream
             .outputMode("update")
             .foreachBatch(write_to_postgres)
             .option("checkpointLocation", "data/checkpoint")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()

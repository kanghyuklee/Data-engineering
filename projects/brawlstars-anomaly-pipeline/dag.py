"""
dag.py — 5회차: Airflow DAG (수집 → 전송 → 전처리 → 품질체크)
================================================================
우리 파이프라인을 매일 자동으로 돌리는 오케스트레이터.
수동으로 하던  producer.py → spark-submit preprocess.py  순서를 Airflow가 묶는다.

흐름:
  fetch_battlelogs → produce_to_kafka → spark_preprocess → quality_check
  (API 수집)         (리플레이 전송)      (전처리+저장+DLQ)    (결과 검증)
  * 어느 Task든 실패하면 Discord 알림(on_failure_callback)

설계 포인트 (5회차 과제 3대 항목):
  ① DAG 구조     : 위 선형 체인 (각 Task = 우리 실제 스크립트 1개)
  ② retry·backoff: 인프라성 Task(fetch/produce/preprocess)는 재시도 O(지수 백오프),
                   데이터 품질 Task(quality_check)는 재시도 X → 바로 알림
  ③ 스케줄·날짜  : @daily. 리플레이 데이터라 '데이터 날짜(event_time)'와
                   '실행 날짜(logical_date)'를 분리 (아래 참고)

멱등성(보너스): preprocess가 overwrite(truncate)+event_id 중복제거라
             같은 날 두 번 돌려도 detection_result 결과가 같다.

리플레이 날짜 매핑: 데이터 날짜 = 배틀 발생 시각(event_time), 실행 날짜 = DAG logical_date.
             과거 배틀을 리플레이하므로 둘은 분리된다(특정 날짜 backfill은 target_date 파라미터로).

----------------------------------------------------------------
실행 요건 (중요):
  Airflow 가 프로젝트 스크립트·Kafka·Spark 에 접근할 수 있어야 한다.
  로컬 standalone 실행(자료 권장) 또는 docker-compose airflow 에:
    - 프로젝트 마운트:  ./:/opt/project   (PROJECT_DIR 로 지정)
    - 파이썬 deps:      kafka-python, lz4, requests (producer/fetch 용)
    - Spark 접근:       아래 spark_preprocess 주석 참고 (SparkSubmitOperator 권장)
  docker-compose 의 airflow 서비스가 /opt/airflow/dags/dag.py 로 이 파일을 읽는다.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── 설정 (환경변수로 덮어쓰기 가능) ──────────────────────────────
PROJECT = os.environ.get("PROJECT_DIR", "/opt/project")        # 프로젝트 마운트 경로
PG = dict(host="postgres", port=5432, db="brawlstars", user="brawl", password="brawl")

# ── 공통 재시도 정책: 일시적 실패는 점점 더 기다렸다 다시 ────────────
default_args = {
    "owner": "kanghyuk",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,             # 대기: 1분 → 2분 → 4분
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": None,                    # 아래에서 _notify_discord 로 채움
}


def _notify_discord(context):
    """어느 Task든 (마지막 재시도까지) 실패하면 Discord로 알림. webhook 없으면 조용히 패스."""
    url = os.environ.get("DISCORD_WEBHOOK")
    if not url:
        print("DISCORD_WEBHOOK 미설정 → 알림 생략")
        return
    import json
    import urllib.request
    ti = context["task_instance"]
    msg = (f"🚨 [brawlstars DAG] `{ti.dag_id}.{ti.task_id}` 실패 "
           f"(logical_date={context.get('logical_date')})")
    data = json.dumps({"content": msg}).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:                           # 알림 실패가 DAG를 죽이진 않게
        print(f"Discord 알림 전송 실패: {e}")


default_args["on_failure_callback"] = _notify_discord


def quality_check(**ctx):
    """
    결과 품질 점검. 기준 미달이면 raise → Task 실패 → 알림.
    데이터가 '틀린' 것이므로 재시도해도 똑같다 → 이 Task만 retries=0.
    """
    import psycopg2

    conn = psycopg2.connect(host=PG["host"], port=PG["port"], dbname=PG["db"],
                            user=PG["user"], password=PG["password"])
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*),
                   count(*) FILTER (WHERE anomaly_score IS NULL),
                   count(*) FILTER (WHERE anomaly_score >= 0.5)
            FROM detection_result
        """)
        total, null_cnt, suspicious = cur.fetchone()
    finally:
        conn.close()

    # 1) 비어 있으면 전처리가 실패한 것
    assert total and total > 0, "detection_result 가 비었음 (전처리 실패 의심)"
    # 2) 점수에 null 있으면 스코어 로직 이상
    assert null_cnt == 0, f"anomaly_score NULL {null_cnt}건 (스코어 로직 점검)"
    # 3) 전 계정이 의심이면 임계값/데이터 이상 (전부 이상일 리 없음)
    assert suspicious < total, f"전 계정({total})이 의심으로 분류됨 (임계값/수집 이상 의심)"

    print(f"[품질] detection_result {total}행 · null 0 · 의심 {suspicious}행 — 통과")


with DAG(
    dag_id="brawlstars_anomaly_batch",
    default_args=default_args,
    description="브롤스타즈 이상 계정 탐지 배치 (수집→전송→전처리→품질체크)",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,                                  # 과거분 몰아서 돌릴 땐 True + backfill
    tags=["brawlstars", "anomaly"],
    params={"max_players": 50, "rate": 50},         # UI에서 조정 가능
) as dag:

    # ── ① 수집 (Supercell API) ─────────────────────────────────
    #   API 429·타임아웃은 일시적 → 재시도 O (fetch_battlelogs 에 429 backoff 내장).
    #   토큰 없이 리플레이만 할 거면 이 Task는 스킵 가능 (기존 data/raw 사용).
    t_fetch = BashOperator(
        task_id="fetch_battlelogs",
        bash_command=(f"cd {PROJECT} && "
                      f"python fetch_battlelogs.py --max-players {{{{ params.max_players }}}}"),
    )

    # ── ② 리플레이 전송 → Kafka(bs.battles) ────────────────────
    #   Kafka 연결 일시 끊김 → 재시도 O. event_id 가 결정적이라 재전송해도 중복은 뒤에서 흡수(멱등).
    t_produce = BashOperator(
        task_id="produce_to_kafka",
        bash_command=(f"cd {PROJECT} && "
                      f"python producer.py --in data/raw --rate {{{{ params.rate }}}}"),
    )

    # ── ③ Spark 전처리 + 저장 + DLQ ────────────────────────────
    #   Spark/DB 일시 오류 → 재시도 O. overwrite+truncate 라 재실행해도 결과 동일(멱등).
    #   NOTE: airflow 컨테이너엔 Spark 가 없다. 운영에선 아래 중 하나로 교체:
    #     (a) SparkSubmitOperator (apache-airflow-providers-apache-spark + spark conn)
    #     (b) 호스트에서 standalone airflow 실행 → 이 docker 명령 그대로 사용
    t_preprocess = BashOperator(
        task_id="spark_preprocess",
        bash_command=(
            "docker compose exec -T spark /opt/spark/bin/spark-submit --master 'local[*]' "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.postgresql:postgresql:42.7.3 "
            "/app/preprocess.py"
        ),
    )

    # ── ④ 품질 체크 (실패 시 알림) ─────────────────────────────
    #   데이터 품질 실패는 재시도 무의미 → retries=0 으로 덮어씀.
    t_qc = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        retries=0,
    )

    # 의존성: 수집 → 전송 → 전처리 → 품질체크
    t_fetch >> t_produce >> t_preprocess >> t_qc

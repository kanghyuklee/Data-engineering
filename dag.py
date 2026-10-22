"""
dag.py
------
Airflow DAG 스켈레톤. 배틀로그 수집부터 집계·품질체크까지의 배치 흐름을 묶는다.
의존성 체인 + 재시도/백오프 + backfill(날짜 파라미터)을 Airflow로 처리하는 게 핵심.

docker-compose의 airflow 서비스가 /opt/airflow/dags/dag.py 로 마운트해서 읽는다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "kanghyuk",
    "retries": 3,                                  # 실패해도 스스로 재시도
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,             # 재시도 간격을 점점 늘림
}


def fetch_battlelogs(**ctx):
    # TODO: fetch_battlelogs.crawl() 호출. rate limit 고려해 분할 수집
    ...


def inject_anomalies(**ctx):
    # TODO: anomaly_injector 로 가짜 이상 계정 섞기 (라벨)
    ...


def load_to_parquet(**ctx):
    # TODO: 수집 JSON을 이벤트로 변환해 Parquet 적재 (또는 consumer 결과 정리)
    ...


def daily_aggregate(**ctx):
    # TODO: 계정별 일배치 집계(승률·연승·트로피 추이) → PostgreSQL
    ...


def quality_check(**ctx):
    # TODO: 건수/널값/중복 event_id 체크. 기준 미달이면 raise 해서 알림
    ...


with DAG(
    dag_id="brawlstars_anomaly_batch",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,                                  # backfill 할 땐 True로
    tags=["brawlstars", "anomaly"],
) as dag:

    t_fetch = PythonOperator(task_id="fetch_battlelogs", python_callable=fetch_battlelogs)
    t_inject = PythonOperator(task_id="inject_anomalies", python_callable=inject_anomalies)
    t_load = PythonOperator(task_id="load_to_parquet", python_callable=load_to_parquet)
    t_agg = PythonOperator(task_id="daily_aggregate", python_callable=daily_aggregate)
    t_qc = PythonOperator(task_id="quality_check", python_callable=quality_check)

    # 의존성 체인: 수집 → 이상주입 → 적재 → 집계 → 품질체크
    t_fetch >> t_inject >> t_load >> t_agg >> t_qc

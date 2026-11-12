"""
upsert_detection.py — 스테이징 → detection_result 병합(UPSERT)

preprocess.py(Spark)가 이번 실행분을 detection_result_staging 에 적재하면,
이 스크립트가 detection_result 로 ON CONFLICT(player_tag) DO UPDATE 병합한다.

왜 upsert 인가 (기존 overwrite+truncate 대체):
  - 기존엔 매 실행마다 detection_result 를 비우고 그 실행분만 다시 넣어,
    이번 배치에 없는 계정은 사라졌다 → 날짜별 누적이 안 됨.
  - upsert 는 새 계정은 INSERT, 기존 계정은 '전처리가 소유한 컬럼'만 UPDATE 한다.
    → 배치에 없던 계정은 그대로 남고(누적), ml_score·max_opp_repeat·final_score·
      flagged_by(이후 단계가 채우는 컬럼)는 건드리지 않아 보존된다.

실행:
  PG_HOST=postgres python upsert_detection.py     # airflow 컨테이너(DAG)
  PG_HOST=localhost python upsert_detection.py     # 호스트에서 수동
"""

import os

import psycopg2

PG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB", "brawlstars"),
    user=os.environ.get("PG_USER", "brawl"),
    password=os.environ.get("PG_PASSWORD", "brawl"),
)
STAGING = "detection_result_staging"

# 전처리(preprocess.py)가 소유한 컬럼만 UPDATE 한다.
#   ml_score·max_opp_repeat·final_score·flagged_by 는 이후 단계(ml/mf/combine)가 채우므로 제외.
BASE_COLS = ["battles", "win_rate", "trophy_gain", "brawler_variety",
             "anomaly_score", "anomaly_reason", "is_injected", "updated_at"]
ALL_COLS = ["player_tag"] + BASE_COLS


def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()

    cols = ", ".join(ALL_COLS)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in BASE_COLS)
    cur.execute(
        f"INSERT INTO detection_result ({cols}) "
        f"SELECT {cols} FROM {STAGING} "
        f"ON CONFLICT (player_tag) DO UPDATE SET {set_clause}"
    )
    conn.commit()

    cur.execute(f"SELECT count(*) FROM {STAGING}")
    staged = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM detection_result")
    total = cur.fetchone()[0]
    print(f"[upsert] 스테이징 {staged}건 병합 → detection_result 누적 {total}계정")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

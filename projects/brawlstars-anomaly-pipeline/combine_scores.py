"""
combine_scores.py — 룰·ML·승부조작 신호를 하나의 final_score로 통합
----------------------------------------------------------------
세 탐지는 서로 다른 유형을 잡는다(룰=단순 지표, ML=복합 outlier, 승부조작=계정 간 공모).
중복이 아니라 상호보완이므로, '어느 하나라도 강하게 의심하면 의심'인 MAX로 합친다.
가중합은 한 신호만 강한 계정을 희석시켜 부적합.

  final_score = max(anomaly_score, ml_score, mf_norm)
  mf_norm     = min(max_opp_repeat / MF_CAP, 1.0)   # 횟수(0~25)를 0~1로
  flagged_by  = 임계 넘은 신호 이름들(rule/ml/mf) → '왜 의심인지' 설명

입력/출력: PostgreSQL detection_result (세 점수 읽어 final_score·flagged_by 갱신)
실행: python combine_scores.py   (ml_detect·match_fixing 다음에)
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
MF_CAP = float(os.environ.get("MF_CAP", "10"))     # 승부조작 정규화 상한(이 횟수면 1.0)
RULE_TH = float(os.environ.get("RULE_TH", "0.6"))  # 룰 의심 임계
ML_TH = float(os.environ.get("ML_TH", "0.5"))      # ML 의심 임계
MF_TH = int(os.environ.get("MF_TH", "5"))          # 승부조작 의심 임계(횟수)


def combine_one(rule, ml, mf_cnt, *, mf_cap=MF_CAP, rule_th=RULE_TH, ml_th=ML_TH, mf_th=MF_TH):
    """세 신호(룰·ML·승부조작)를 MAX로 통합하고, 임계 넘은 신호 이름을 함께 반환.
    가중합은 한 신호만 강한 계정을 희석시켜 부적합 → MAX. 순수 함수(DB 불필요, 테스트 대상)."""
    rule = rule or 0.0
    ml = ml or 0.0
    mf_cnt = mf_cnt or 0
    mf_norm = min(mf_cnt / mf_cap, 1.0)          # 횟수(0~)를 0~1로 정규화
    final = max(rule, ml, mf_norm)
    flags = []
    if rule >= rule_th:
        flags.append("rule")
    if ml >= ml_th:
        flags.append("ml")
    if mf_cnt >= mf_th:
        flags.append("mf")
    return round(final, 3), ",".join(flags) or "none"


def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute("ALTER TABLE detection_result ADD COLUMN IF NOT EXISTS final_score DOUBLE PRECISION")
    cur.execute("ALTER TABLE detection_result ADD COLUMN IF NOT EXISTS flagged_by TEXT")

    cur.execute("SELECT player_tag, anomaly_score, ml_score, max_opp_repeat FROM detection_result")
    rows = cur.fetchall()

    updates = []
    for tag, rule, ml, mf in rows:
        final, flagged = combine_one(rule, ml, mf)
        updates.append((final, flagged, tag))

    cur.executemany(
        "UPDATE detection_result SET final_score=%s, flagged_by=%s WHERE player_tag=%s",
        updates)
    conn.commit()

    # 출력: flagged_by 분포 + final_score 상위
    cur.execute("SELECT flagged_by, count(*) FROM detection_result "
                "GROUP BY flagged_by ORDER BY 2 DESC")
    print(f"총 {len(rows)}계정 통합 (MAX · 임계 rule>={RULE_TH} ml>={ML_TH} mf>={MF_TH})")
    print("\n=== flagged_by 분포 (어느 신호에 걸렸나) ===")
    for fb, c in cur.fetchall():
        print(f"  {fb:<12} {c}")

    cur.execute(
        "SELECT player_tag, round(final_score::numeric,3), flagged_by, "
        "round(anomaly_score::numeric,2), round(ml_score::numeric,2), max_opp_repeat "
        "FROM detection_result ORDER BY final_score DESC NULLS LAST LIMIT 10")
    print("\n=== final_score 상위 10 (어느 신호인지 함께) ===")
    print(f"{'player_tag':<12}{'final':>6} {'flagged_by':<12}{'rule':>5}{'ml':>5}{'mf':>4}")
    for tag, fs, fb, r, m, mf in cur.fetchall():
        print(f"{tag:<12}{fs!s:>6} {fb:<12}{r!s:>5}{m!s:>5}{(mf or 0)!s:>4}")

    conn.close()


if __name__ == "__main__":
    main()

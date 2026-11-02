"""
ml_detect.py — IsolationForest 기반 이상 계정 탐지 (룰 기반 보완)
----------------------------------------------------------------
룰 기반 점수(anomaly_score)는 "승률>0.9" / "브롤러≤1" 같은 단일 조건이라,
여러 지표가 복합적으로 튀는 계정은 놓칠 수 있다. 여기서는 비지도 학습
(IsolationForest)으로 다차원 피처에서 '튀는' 계정을 outlier로 잡아
ml_score(0~1)를 매긴다. 두 점수를 나란히 두고 비교한다.

입력: PostgreSQL detection_result (계정별 집계 피처)
모델: IsolationForest (비지도, 정답 라벨 불필요)
출력: detection_result.ml_score 갱신 + 룰 vs ML 비교 출력

실행 (호스트):
  pip install scikit-learn numpy psycopg2-binary
  python ml_detect.py
"""

import os

import numpy as np
import psycopg2
from sklearn.ensemble import IsolationForest

PG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname="brawlstars", user="brawl", password="brawl",
)
FEATURES = ["battles", "win_rate", "trophy_gain", "brawler_variety"]
CONTAMINATION = float(os.environ.get("ML_CONTAMINATION", "0.1"))  # 이상으로 볼 대략 비율


def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()

    # ── 1) 피처 읽기 ────────────────────────────────────────────
    cur.execute(f"SELECT player_tag, {', '.join(FEATURES)}, anomaly_reason "
                f"FROM detection_result")
    rows = cur.fetchall()
    if not rows:
        print("detection_result 가 비었음 — preprocess.py 먼저 실행하세요.")
        return
    tags = [r[0] for r in rows]
    reasons = [r[-1] for r in rows]
    X = np.array([[float(v) if v is not None else 0.0 for v in r[1:-1]] for r in rows])

    # ── 2) 표준화 (스케일이 다른 피처를 동일 선상에) ─────────────
    mu, sigma = X.mean(axis=0), X.std(axis=0) + 1e-9
    Xs = (X - mu) / sigma

    # ── 3) IsolationForest 학습 → 이상도 ────────────────────────
    #   score_samples: 클수록 정상 → 부호 반전해 '높을수록 이상'으로.
    model = IsolationForest(n_estimators=300, contamination=CONTAMINATION,
                            random_state=42)
    model.fit(Xs)
    raw = -model.score_samples(Xs)
    ml = (raw - raw.min()) / (raw.max() - raw.min() + 1e-9)  # 0~1 정규화

    # ── 4) detection_result.ml_score 갱신 ───────────────────────
    cur.execute("ALTER TABLE detection_result "
                "ADD COLUMN IF NOT EXISTS ml_score DOUBLE PRECISION")
    cur.executemany("UPDATE detection_result SET ml_score=%s WHERE player_tag=%s",
                    [(float(s), t) for s, t in zip(ml, tags)])
    conn.commit()

    # ── 5) 룰 vs ML 비교 출력 ───────────────────────────────────
    n = len(tags)
    order = np.argsort(-ml)
    topk = max(1, int(n * CONTAMINATION))
    ml_flag = set(tags[i] for i in order[:topk])
    rule_flag = set(t for t, r in zip(tags, reasons) if r in ("bot", "boosting"))
    overlap = ml_flag & rule_flag

    print(f"총 {n}계정 · ML 이상 상위 {topk}개(상위 {CONTAMINATION:.0%}) 기준")
    print(f"  룰 탐지(bot/boosting): {len(rule_flag)}개")
    print(f"  ML 탐지              : {len(ml_flag)}개")
    print(f"  둘 다 지목(교집합)    : {len(overlap)}개  "
          f"→ ML만 새로 지목: {len(ml_flag - rule_flag)}개 / 룰만: {len(rule_flag - ml_flag)}개")
    print()
    print("=== ML 이상도 상위 10 (룰 사유와 비교) ===")
    print(f"{'player_tag':<12} {'ml':>5} {'battles':>7} {'win':>5} {'variety':>7}  rule")
    for i in order[:10]:
        print(f"{tags[i]:<12} {ml[i]:>5.3f} {int(X[i][0]):>7} "
              f"{X[i][1]:>5.2f} {int(X[i][3]):>7}  {reasons[i]}")

    conn.close()


if __name__ == "__main__":
    main()

"""
match_fixing.py — 그래프 기반 승부조작/대리 의심 (상대 반복 쌍)
----------------------------------------------------------------
정상 매칭은 매번 다른 상대와 붙는다. 특정 상대를 '비정상적으로 자주' 만나면
짜고 하는 승부조작/대리 의심 신호다. 정제 이벤트의 opponents(상대 팀) 배열에서
(계정 A, 상대 B) 쌍의 빈도를 세어, 한 상대를 많이 만난 계정을 표시한다.

입력: Parquet 정제 이벤트 (player_tag, opponents[])
로직: (A,B) 쌍 카운트 → 계정별 '한 상대 최대 반복(max_opp_repeat)'
출력: 반복 쌍 상위 + 의심 계정 + detection_result.max_opp_repeat 갱신

실행 (호스트):
  pip install pyarrow psycopg2-binary   # 이미 설치돼 있으면 생략
  python match_fixing.py

한계: 상위 트로피대는 표본 풀이 작아 자연 반복도 있음 → 임계값(MF_THRESHOLD) 튜닝 필요.
      정밀하게는 반복 쌍의 '승/패가 한쪽으로 쏠리는지'까지 보면 더 확실(TODO).
"""

import os
from collections import Counter, defaultdict

import psycopg2
import pyarrow.dataset as ds

PARQUET = os.environ.get("OUT_PARQUET_HOST", "data/parquet/battles")
PG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB", "brawlstars"),
    user=os.environ.get("PG_USER", "brawl"),
    password=os.environ.get("PG_PASSWORD", "brawl"),
)
THRESHOLD = int(os.environ.get("MF_THRESHOLD", "5"))  # 같은 상대 이 횟수 이상 = 의심
# 매칭 기반 대전만 분석. 친선전(friendly)·토너먼트·챌린지는 상대를 직접 고를 수 있어
#   같은 상대 반복이 정상 → 승부조작 신호가 아님 → 제외해야 오탐이 안 생긴다.
MATCHMADE = set(os.environ.get("MF_TYPES", "ranked,soloRanked").split(","))


def opponent_pairs(rows, matchmade=MATCHMADE):
    """정제 이벤트에서 (계정 A, 상대 B) 쌍의 빈도를 센다.
    매칭전(ranked/soloRanked)만 — 친선전·토너먼트는 상대를 고를 수 있어 제외. 자기 자신 제외.
    순수 함수(Parquet·DB 불필요, 테스트 대상)."""
    pair = Counter()
    for r in rows:
        if r["type"] not in matchmade:
            continue
        a, opps = r["player_tag"], (r["opponents"] or [])
        for b in opps:
            if b and b != a:
                pair[(a, b)] += 1
    return pair


def max_repeat_by_account(pair):
    """계정별 '한 상대를 최대 몇 번 만났나'와 그 상대를 반환. 순수 함수."""
    max_rep, max_opp = defaultdict(int), {}
    for (a, b), c in pair.items():
        if c > max_rep[a]:
            max_rep[a], max_opp[a] = c, b
    return max_rep, max_opp


def main():
    t = (ds.dataset(PARQUET, format="parquet", partitioning="hive")
         .to_table(columns=["player_tag", "opponents", "type"]))
    rows = t.to_pylist()

    # (A, B) = A가 B를 상대로 만난 횟수 (매칭전만 — 친선전·토너먼트 제외)
    pair = opponent_pairs(rows)
    used = sum(1 for r in rows if r["type"] in MATCHMADE)

    print(f"전체 {len(rows)}건 중 매칭전({'/'.join(sorted(MATCHMADE))}) {used}건만 사용 "
          f"(친선전·토너먼트·챌린지 제외)")

    # 계정별 '한 상대를 최대 몇 번 만났나' + 그 상대
    max_rep, max_opp = max_repeat_by_account(pair)

    print(f"이벤트 {len(rows)}건 · 상대 쌍 {len(pair)}개 · 의심 임계 {THRESHOLD}회")

    print("\n=== 가장 자주 만난 상대 쌍 상위 15 ===")
    print(f"{'player_tag':<12} {'opponent':<12} {'횟수':>4}")
    for (a, b), c in pair.most_common(15):
        print(f"{a:<12} {b:<12} {c:>4}")

    suspects = {a: v for a, v in max_rep.items() if v >= THRESHOLD}
    print(f"\n=== 승부조작 의심 (같은 상대 {THRESHOLD}회 이상): {len(suspects)}계정 ===")
    print(f"{'player_tag':<12} {'최다상대':<12} {'횟수':>4}")
    for a, v in sorted(suspects.items(), key=lambda x: -x[1])[:10]:
        print(f"{a:<12} {max_opp[a]:<12} {v:>4}")

    # detection_result.max_opp_repeat 갱신 (승부조작 신호 컬럼)
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute("ALTER TABLE detection_result "
                "ADD COLUMN IF NOT EXISTS max_opp_repeat INT")
    cur.executemany("UPDATE detection_result SET max_opp_repeat=%s WHERE player_tag=%s",
                    [(v, a) for a, v in max_rep.items()])
    conn.commit()
    conn.close()
    print(f"\ndetection_result.max_opp_repeat 갱신 ({len(max_rep)}계정)")


if __name__ == "__main__":
    main()

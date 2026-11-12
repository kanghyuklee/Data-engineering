"""통합 점수·승부조작 탐지의 순수 로직 단위 테스트 (DB·Spark 불필요).

combine_scores.combine_one / match_fixing.opponent_pairs·max_repeat_by_account만 검증한다.
세 함수는 I/O에서 분리된 순수 함수라 CI에서 빠르게 돌릴 수 있다.
"""

from combine_scores import combine_one
from match_fixing import max_repeat_by_account, opponent_pairs


# ── combine_scores.combine_one ──────────────────────────────────────
def test_통합점수는_가중합이_아니라_MAX():
    # 룰은 약하고 ML만 강한 계정 → final은 ML 값(희석 없음)
    final, flagged = combine_one(0.1, 0.8, 0)
    assert final == 0.8
    assert flagged == "ml"


def test_승부조작_정규화는_상한_1로_클리핑():
    # mf 12회, cap 10 → mf_norm = 1.0 (1.0 초과 안 함)
    final, flagged = combine_one(0.1, 0.2, 12, mf_cap=10)
    assert final == 1.0
    assert "mf" in flagged


def test_임계_넘은_신호만_flagged_by에_표시():
    final, flagged = combine_one(0.8, 0.6, 7)  # 세 신호 모두 임계 이상
    assert final == 0.8
    assert flagged.split(",") == ["rule", "ml", "mf"]


def test_모두_임계_미만이면_none():
    _, flagged = combine_one(0.1, 0.2, 2)
    assert flagged == "none"


def test_DB_NULL은_0으로_처리():
    final, flagged = combine_one(None, None, None)
    assert final == 0.0
    assert flagged == "none"


# ── match_fixing ────────────────────────────────────────────────────
ROWS = [
    {"player_tag": "#A", "opponents": ["#B", "#C"], "type": "ranked"},
    {"player_tag": "#A", "opponents": ["#B"], "type": "ranked"},
    {"player_tag": "#A", "opponents": ["#B"], "type": "friendly"},   # 친선전 → 제외돼야
    {"player_tag": "#A", "opponents": ["#A", None], "type": "ranked"},  # 자기 자신·None 제외
]
MATCHMADE = {"ranked", "soloRanked"}


def test_매칭전만_세고_자기자신은_제외():
    pair = opponent_pairs(ROWS, matchmade=MATCHMADE)
    assert pair[("#A", "#B")] == 2        # friendly 1건은 세지 않음
    assert pair[("#A", "#C")] == 1
    assert ("#A", "#A") not in pair       # 자기 자신 제외
    assert ("#A", None) not in pair       # None 제외


def test_계정별_최다_반복_상대를_고름():
    pair = opponent_pairs(ROWS, matchmade=MATCHMADE)
    max_rep, max_opp = max_repeat_by_account(pair)
    assert max_rep["#A"] == 2
    assert max_opp["#A"] == "#B"

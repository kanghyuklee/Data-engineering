"""
make_sample.py
--------------
토큰 없이도 파이프라인을 돌려볼 수 있게, 브롤스타즈 API와 '똑같은 모양'의
가짜 배틀로그 JSON을 만들어 data/raw/ 에 저장한다.

- 정상 계정: 모드·브롤러·결과·플레이 간격이 다양함
- 이상 계정(일부): 같은 브롤러·거의 승리·일정한 간격 (봇/부스팅처럼) → "_injected": 1 라벨

실행 예:
  python make_sample.py --players 50 --out data/sample
그다음:
  python producer.py --in data/sample --rate 50

주의: 실데이터(fetch_battlelogs)는 data/raw 에 쌓이므로, 샘플은 data/sample 로 분리해 섞이지 않게 한다.
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta, timezone

MODES = ["gemGrab", "brawlBall", "showdown", "heist", "bounty", "hotZone"]
MAPS = ["Hard Rock Mine", "Backyard Bowl", "Skull Creek", "Safe Zone", "Shooting Star"]
BRAWLERS = [
    (16000000, "SHELLY"), (16000001, "COLT"), (16000002, "BULL"),
    (16000003, "BROCK"), (16000006, "SPIKE"), (16000008, "DYNAMIKE"),
    (16000010, "PIPER"), (16000011, "PAM"), (16000014, "LEON"),
]


def rand_tag() -> str:
    chars = "0289PYLQGRJCUV"
    return "#" + "".join(random.choice(chars) for _ in range(8))


def a_player(tag: str, brawler=None) -> dict:
    bid, bname = brawler or random.choice(BRAWLERS)
    return {
        "tag": tag,
        "name": "player_" + tag[1:4],
        "brawler": {"id": bid, "name": bname, "power": random.randint(7, 11),
                    "trophies": random.randint(300, 900)},
    }


def battle(owner: str, t: datetime, *, bot=False, boost=False) -> dict:
    mode = MODES[0] if (bot or boost) else random.choice(MODES)
    # 이상 계정: 같은 브롤러 고정 + 거의 승리
    owner_brawler = BRAWLERS[0] if (bot or boost) else random.choice(BRAWLERS)
    if bot or boost:
        result = "victory" if random.random() < 0.95 else "defeat"
        trophy = 9 if boost else random.choice([7, 8, 9])
    else:
        result = random.choice(["victory", "defeat", "defeat", "victory", "draw"])
        trophy = {"victory": random.randint(4, 9),
                  "defeat": -random.randint(3, 8), "draw": 0}[result]

    me = a_player(owner, owner_brawler)
    team_mates = [a_player(rand_tag()) for _ in range(2)]
    opponents = [a_player(rand_tag()) for _ in range(3)]
    b = {
        "battleTime": t.strftime("%Y%m%dT%H%M%S.000Z"),
        "event": {"id": random.randint(15000000, 15000200),
                  "mode": mode, "map": random.choice(MAPS)},
        "battle": {
            "mode": mode, "type": "ranked", "result": result,
            "duration": random.randint(60, 150), "trophyChange": trophy,
            "starPlayer": me,
            "teams": [[me] + team_mates, opponents],
        },
    }
    if bot or boost:
        b["_injected"] = 1   # 정답 라벨 (anomaly_injector 없이 바로 섞어 둠)
    return b


def gen_player(owner: str, kind: str) -> list:
    """kind: 'normal' | 'bot' | 'boost'. 최근 25전 생성."""
    battles, t = [], datetime.now(timezone.utc)
    for _ in range(25):
        if kind == "bot":
            t -= timedelta(minutes=3)                       # 거의 일정한 간격
            battles.append(battle(owner, t, bot=True))
        elif kind == "boost":
            t -= timedelta(minutes=random.randint(2, 6))
            battles.append(battle(owner, t, boost=True))
        else:
            t -= timedelta(minutes=random.randint(5, 180))  # 들쭉날쭉
            battles.append(battle(owner, t))
    return battles


def run(n_players: int, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    n_bad = max(1, n_players // 20)   # 약 5%를 이상 계정으로
    for i in range(n_players):
        tag = rand_tag()
        if i < n_bad:
            kind = "bot" if i % 2 == 0 else "boost"
        else:
            kind = "normal"
        battles = gen_player(tag, kind)
        with open(os.path.join(out_dir, f"{tag.strip('#')}.json"), "w", encoding="utf-8") as f:
            json.dump(battles, f, ensure_ascii=False)
    print(f"done. players={n_players} (이상 {n_bad}명), 배틀={n_players*25} -> {out_dir}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--players", type=int, default=50)
    p.add_argument("--out", default="data/sample")
    args = p.parse_args()
    run(args.players, args.out)


if __name__ == "__main__":
    main()

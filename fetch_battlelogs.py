"""
fetch_battlelogs.py
-------------------
브롤스타즈 공식 API에서 배틀로그를 모아 JSON 파일로 저장한다.
랭킹에서 시작 태그를 받고 → 각 태그의 배틀로그를 받고 → 거기 나온 상대 태그로 넓혀가는 크롤 방식.

사전 준비:
  1) developer.brawlstars.com 에서 토큰 발급 (토큰은 발급 시 등록한 IP에서만 동작)
  2) 토큰을 환경변수로:
       Windows:  set BRAWLSTARS_API_TOKEN=발급받은토큰
       mac/linux: export BRAWLSTARS_API_TOKEN=...
  3) pip install requests

먼저 토큰이 잘 되는지 1명만 테스트:
  python fetch_battlelogs.py --tag "#2PP0PLG"      # 아무 공개 태그나

본격 수집:
  python fetch_battlelogs.py --country global --seeds 200 --max-players 2000 --rps 3 --out data/raw
"""

import argparse
import json
import os
import sys
import time
from collections import deque
from urllib.parse import quote

import requests

BASE = "https://api.brawlstars.com/v1"


def _load_dotenv(path: str = ".env"):
    """.env 파일이 있으면 KEY=VALUE 를 환경변수로 읽어온다 (추가 패키지 불필요)."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()
TOKEN = os.environ.get("BRAWLSTARS_API_TOKEN", "")

_last_call = [0.0]        # 마지막 호출 시각 (rate limit용)
_min_interval = [0.34]    # 호출 간 최소 간격(초). --rps로 조정


class ApiError(Exception):
    pass


def _get(path: str, max_retries: int = 5) -> dict:
    """GET helper. 태그의 '#'는 %23으로 인코딩. 429/5xx는 backoff 재시도."""
    url = f"{BASE}{path}"
    headers = {"Authorization": f"Bearer {TOKEN}"}

    for attempt in range(max_retries):
        # rate limit: 직전 호출과 최소 간격 유지
        gap = time.time() - _last_call[0]
        if gap < _min_interval[0]:
            time.sleep(_min_interval[0] - gap)
        _last_call[0] = time.time()

        resp = requests.get(url, headers=headers, timeout=10)

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 403:
            raise ApiError(
                "403 Forbidden — 토큰이 없거나 '등록한 IP'와 현재 IP가 다릅니다.\n"
                "   developer.brawlstars.com 에서 현재 IP로 키를 다시 발급하세요."
            )
        if resp.status_code == 404:
            raise ApiError("404 — 해당 태그를 찾을 수 없음 (태그 오타/비공개)")
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"  429 rate limit → {wait:.1f}s 대기 후 재시도")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        raise ApiError(f"{resp.status_code} — {resp.text[:200]}")

    raise ApiError(f"재시도 {max_retries}회 초과: {path}")


def top_player_tags(country: str, n: int) -> list:
    data = _get(f"/rankings/{country}/players")
    return [p["tag"] for p in data.get("items", [])[:n]]


def battlelog(tag: str) -> list:
    return _get(f"/players/{quote(tag)}/battlelog").get("items", [])  # '#'→'%23'


def _tags_in_battle(b: dict) -> list:
    """팀 모드(teams)와 솔로 모드(players) 둘 다에서 참가자 태그 추출."""
    out = []
    bat = b.get("battle", {})
    for team in bat.get("teams", []) or []:
        out += [p["tag"] for p in team]
    for p in bat.get("players", []) or []:   # 쇼다운 등
        out.append(p["tag"])
    return out


def crawl(seeds: list, max_players: int, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    seen, q = set(), deque(seeds)
    saved = 0

    while q and saved < max_players:
        tag = q.popleft()
        if tag in seen:
            continue
        seen.add(tag)
        try:
            battles = battlelog(tag)
        except ApiError as e:
            print(f"skip {tag}: {e}")
            continue

        with open(os.path.join(out_dir, f"{tag.strip('#')}.json"), "w", encoding="utf-8") as f:
            json.dump(battles, f, ensure_ascii=False)
        saved += 1
        if saved % 50 == 0:
            print(f"  ... {saved}명 저장 (큐 대기 {len(q)})")

        for b in battles:                       # 상대 태그로 큐 확장
            for t in _tags_in_battle(b):
                if t not in seen:
                    q.append(t)

    print(f"done. {saved}명 저장 -> {out_dir}")


def main():
    if not TOKEN:
        sys.exit("환경변수 BRAWLSTARS_API_TOKEN 이 필요합니다. (README 1단계 참고)")

    p = argparse.ArgumentParser()
    p.add_argument("--tag", help="이거 하나만 주면 단일 태그 테스트 (토큰 확인용)")
    p.add_argument("--country", default="global")
    p.add_argument("--seeds", type=int, default=200, help="랭킹 상위 몇 명에서 시작할지")
    p.add_argument("--max-players", type=int, default=2000)
    p.add_argument("--rps", type=float, default=3.0, help="초당 요청 수 (낮출수록 안전)")
    p.add_argument("--out", default="data/raw")
    args = p.parse_args()

    _min_interval[0] = 1.0 / max(0.1, args.rps)

    # 단일 태그 테스트 모드
    if args.tag:
        battles = battlelog(args.tag)
        print(f"OK — {args.tag} 배틀 {len(battles)}건")
        if battles:
            print(json.dumps(battles[0], ensure_ascii=False, indent=2)[:800])
        return

    seeds = top_player_tags(args.country, args.seeds)
    print(f"시작 태그 {len(seeds)}명 확보, 크롤 시작 (rps={args.rps})")
    crawl(seeds, args.max_players, args.out)


if __name__ == "__main__":
    main()

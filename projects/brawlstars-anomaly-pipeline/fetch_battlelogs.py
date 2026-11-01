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

본격 수집 (동시 요청):
  python fetch_battlelogs.py --country global --seeds 200 --max-players 2000 --rps 10 --workers 12 --out data/raw
  # --workers 로 동시에 여러 요청을 날려 처리량↑, --rps 로 전체 속도 상한(429 예방).
  # 중간에 끊겨도 같은 명령 재실행하면 이미 받은 계정은 건너뛰고 이어받음.
"""

import argparse
import json
import os
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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

_last_call = [0.0]            # 마지막 호출 시각 (rate limit용)
_min_interval = [0.34]        # 호출 간 최소 간격(초). --rps로 조정
_rate_lock = threading.Lock() # 동시 요청에서도 전체 호출 간격을 지키게 (429 예방)


class ApiError(Exception):
    pass


def _get(path: str, max_retries: int = 5) -> dict:
    """GET helper. 태그의 '#'는 %23으로 인코딩. 429/5xx/네트워크 오류는 backoff 재시도."""
    url = f"{BASE}{path}"
    headers = {"Authorization": f"Bearer {TOKEN}"}

    for attempt in range(max_retries):
        # rate limit: 직전 호출과 최소 간격 유지 (lock으로 동시 요청에서도 간격 보장).
        #   sleep은 lock 안에서 '순번'만 맞추고, 실제 네트워크 대기는 lock 밖이라 요청끼리 겹침.
        with _rate_lock:
            gap = time.time() - _last_call[0]
            if gap < _min_interval[0]:
                time.sleep(_min_interval[0] - gap)
            _last_call[0] = time.time()

        try:
            # timeout=(연결 5s, 읽기 20s). 네트워크가 느려도 읽기는 20s까지 기다림.
            resp = requests.get(url, headers=headers, timeout=(5, 20))
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # 타임아웃·연결 끊김은 '일시 장애'다 → backoff 후 재시도.
            # 끝까지 실패하면 ApiError로 올려서(아래 for 종료 후) 크롤이 '이 태그 하나만'
            # 건너뛰게 한다. 과거엔 이 예외가 안 잡혀 크롤 전체가 죽고 큐(수만 건)가 날아갔음.
            wait = 2 ** attempt
            print(f"  네트워크 오류({type(e).__name__}) → {wait}s 후 재시도 ({attempt+1}/{max_retries})")
            time.sleep(wait)
            continue

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


def crawl(seeds: list, max_players: int, out_dir: str, workers: int = 12):
    """동시 요청(ThreadPool) BFS 크롤.

    - workers개를 동시에 날려 네트워크 왕복 대기를 겹친다 → 동기 방식의 '초당 1건' 병목 해소.
    - 전체 요청 속도는 _get의 rate limit(--rps)이 스레드 안전하게 상한 → 429 예방.
    - 이미 받은 계정은 디스크에서 읽어 큐만 확장(재개). 중간에 죽어도 재실행하면 이어받음.
    - 한도(max_players, 신규 저장 수) 도달 시 새 작업 투입을 멈추고 진행 중인 것만 마무리.
    """
    os.makedirs(out_dir, exist_ok=True)
    seen, seen_lock = set(), threading.Lock()
    q, q_lock = deque(dict.fromkeys(seeds)), threading.Lock()   # 시드 중복 제거
    saved_box, saved_lock = [0], threading.Lock()

    def take(tag: str) -> bool:
        """아직 안 본 태그면 seen에 등록하고 True (중복 작업 방지)."""
        with seen_lock:
            if tag in seen:
                return False
            seen.add(tag)
            return True

    def reached_cap() -> bool:
        with saved_lock:
            return saved_box[0] >= max_players

    def process(tag: str) -> list:
        """한 계정 처리 → 큐에 넣을 자식(상대) 태그 리스트 반환. 디스크 우선, 없으면 API."""
        fpath = os.path.join(out_dir, f"{tag.strip('#')}.json")
        battles = None
        if os.path.exists(fpath):
            try:
                with open(fpath, encoding="utf-8") as f:
                    battles = json.load(f)       # 이미 받음 → API 생략(재개)
            except (json.JSONDecodeError, OSError):
                battles = None                   # 깨진 파일 → 새로 받음
        if battles is None:
            try:
                battles = battlelog(tag)
            except ApiError as e:
                print(f"skip {tag}: {e}")
                return []
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(battles, f, ensure_ascii=False)
            with saved_lock:
                saved_box[0] += 1
                s = saved_box[0]
            if s % 50 == 0:
                print(f"  ... {s}명 신규 저장 (동시 {workers} / 큐 {len(q)})")

        out = []
        for b in battles:
            out += _tags_in_battle(b)
        return out

    with ThreadPoolExecutor(max_workers=workers) as ex:
        pending = set()

        def fill():
            # in-flight를 workers*2까지 채움 (큐가 비거나 한도 도달하면 멈춤)
            while len(pending) < workers * 2 and not reached_cap():
                with q_lock:
                    tag = q.popleft() if q else None
                if tag is None:
                    break
                if take(tag):
                    pending.add(ex.submit(process, tag))

        fill()
        while pending:
            done_set, rest = wait(pending, return_when=FIRST_COMPLETED)
            pending = set(rest)
            for fut in done_set:
                children = fut.result()
                if children and not reached_cap():
                    with q_lock:
                        q.extend(children)          # 상대 태그로 큐 확장
            if not reached_cap():
                fill()

    print(f"done. 이번 실행 신규 {saved_box[0]}명 저장 -> {out_dir} "
          f"(디렉터리 총 {len(os.listdir(out_dir))}개 파일)")


def main():
    if not TOKEN:
        sys.exit("환경변수 BRAWLSTARS_API_TOKEN 이 필요합니다. (README 1단계 참고)")

    p = argparse.ArgumentParser()
    p.add_argument("--tag", help="이거 하나만 주면 단일 태그 테스트 (토큰 확인용)")
    p.add_argument("--country", default="global")
    p.add_argument("--seeds", type=int, default=200, help="랭킹 상위 몇 명에서 시작할지")
    p.add_argument("--max-players", type=int, default=2000,
                   help="이번 실행에서 '신규로' 받을 계정 수 (이미 받은 건 건너뜀). 재실행하면 이어서 더 모임")
    p.add_argument("--rps", type=float, default=3.0,
                   help="초당 요청 수 상한 (전체 합. 높일수록 빠르나 429 위험)")
    p.add_argument("--workers", type=int, default=12,
                   help="동시 요청 수 (네트워크 왕복 대기를 겹쳐 처리량↑). rps×왕복(~1s) 정도면 충분")
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
    print(f"시작 태그 {len(seeds)}명 확보, 크롤 시작 (rps={args.rps}, workers={args.workers})")
    crawl(seeds, args.max_players, args.out, args.workers)


if __name__ == "__main__":
    main()

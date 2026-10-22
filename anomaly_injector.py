"""
anomaly_injector.py
-------------------
정상 배틀로그 일부를 바탕으로 '봇·부스팅·승부조작처럼' 보이는 가짜 이상 계정 배틀을 만든다.
is_injected=1 라벨을 심어두면 나중에 탐지 정확도(precision/recall)를 평가할 수 있다.

replay_producer가 읽는 JSON과 같은 포맷으로 내보내되, 각 배틀 dict에 "_injected": 1 을 넣는다.

실행 예:
  python anomaly_injector.py --in data/raw --out data/raw_mixed --ratio 0.05
"""

import argparse
import copy
import glob
import json
import os


def make_bot_account(battles: list[dict]) -> list[dict]:
    """
    봇처럼 보이게: 같은 모드·같은 브롤러만 반복하고 결과를 거의 승리로.
    (replay 단계에서 battleTime 간격이 균일하면 '간격 분산≈0'으로도 드러남)
    """
    out = []
    for b in battles:
        nb = copy.deepcopy(b)
        nb["_injected"] = 1
        nb.setdefault("battle", {})["result"] = "victory"
        # TODO: mode/brawler를 하나로 고정, trophyChange를 일정하게
        out.append(nb)
    return out


def make_booster_account(battles: list[dict]) -> list[dict]:
    """부스팅(대리)처럼: 트로피가 급등하도록 trophyChange를 크게/연속 승리로."""
    out = []
    for b in battles:
        nb = copy.deepcopy(b)
        nb["_injected"] = 1
        nb.setdefault("battle", {})["result"] = "victory"
        nb["battle"]["trophyChange"] = 9   # TODO: 비정상적으로 높은 증가 패턴
        out.append(nb)
    return out


def run(in_dir: str, out_dir: str, ratio: float):
    os.makedirs(out_dir, exist_ok=True)
    files = sorted(glob.glob(os.path.join(in_dir, "*.json")))
    n_inject = max(1, int(len(files) * ratio))

    for i, path in enumerate(files):
        with open(path, encoding="utf-8") as f:
            battles = json.load(f)

        # 원본은 그대로 복사
        base = os.path.basename(path)
        with open(os.path.join(out_dir, base), "w", encoding="utf-8") as f:
            json.dump(battles, f, ensure_ascii=False)

        # 앞쪽 일부를 이상 계정 버전으로 추가 생성
        if i < n_inject:
            maker = make_bot_account if i % 2 == 0 else make_booster_account
            fake = maker(battles)
            fname = base.replace(".json", "_inj.json")
            with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
                json.dump(fake, f, ensure_ascii=False)

    print(f"done. files={len(files)}, injected={n_inject}, out={out_dir}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_dir", default="data/raw")
    p.add_argument("--out", dest="out_dir", default="data/raw_mixed")
    p.add_argument("--ratio", type=float, default=0.05, help="이상 계정 주입 비율")
    args = p.parse_args()
    run(args.in_dir, args.out_dir, args.ratio)


if __name__ == "__main__":
    main()

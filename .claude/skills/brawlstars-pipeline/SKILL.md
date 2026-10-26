---
name: brawlstars-pipeline
description: 메타코드 데엔 7기 A주제 "Brawl Stars 이상 계정 탐지 파이프라인" 프로젝트를 이어서 작업할 때 사용. 프로젝트 맥락·실행 방법·git 워크플로우·설계 개념을 담고 있어, 다른 PC에서 repo를 clone해 이어갈 때 이 skill을 따른다. 코드 수정, 로컬 실행(Kafka 전송/수신), git 커밋·push, 회차 과제 준비, 파이프라인 개념 질문에 활용한다.
---

# Brawl Stars 이상 계정 탐지 파이프라인 — 작업 가이드

메타코드 데이터 엔지니어 7기 A주제(안정적 파이프라인 + 로드 테스트) 스터디 프로젝트.
이 skill은 **다른 PC에서도 동일하게 이어 작업**하기 위한 프로젝트 맥락·실행·git 규칙 모음이다.

## 1. 프로젝트 개요

- **목표**: 브롤스타즈 배틀 데이터로 **봇·대리(트로피 부스팅)·승부조작** 계정을 탐지. 부하에도 유실·중복 없이 버티는 구조가 핵심.
- **실시간 아님**: 실데이터를 **리플레이(N배속 재생)** 하는 방식. 이유 = 외부 API rate limit 때문에 로드 테스트하려면 리플레이 필수.
- **데이터**: Supercell 공식 API (`api.brawlstars.com/v1`, 배틀로그 = 계정당 최근 25전). 공식 토큰 필요(developer.brawlstars.com, IP 등록식).

## 2. 위치와 구조

- GitHub: `github.com/kanghyuklee/Data-engineering`, 프로젝트는 `projects/brawlstars-anomaly-pipeline/`
- 같은 repo에 `projects/gameguard-data-pipeline/`(별개 계획)도 공존 — 건드리지 말 것.
- 파이프라인: `fetch_battlelogs.py`(API 수집→`data/raw` JSON) → `producer.py`(리플레이로 Kafka 전송) → Kafka(`bs.battles`, partition 3, key=player_tag, retention 7일, 실패는 `bs.dlq`) → Spark(탐지, 4회차) → PostgreSQL(결과)+Parquet(원본) → Grafana(모니터링).
- 주요 파일: `producer.py` `fetch_battlelogs.py` `anomaly_injector.py` `consumer.py` `preprocess.py` `dag.py` `schema.sql` `create_topics.sh` `make_sample.py` `docker-compose.yml` `.env.example` `README.md`

## 3. 실행 방법 (Windows + Git Bash 기준)

```bash
cd ".../Data-engineering/projects/brawlstars-anomaly-pipeline"   # 반드시 이 폴더에서
pip install -r requirements.txt           # kafka-python, lz4, requests 등
# 토큰: .env.example 복사 → .env 로, BRAWLSTARS_API_TOKEN=... 입력 (git에 안 올라감)

docker compose up -d kafka                # Kafka 기동
bash create_topics.sh                     # 토픽 생성 (bs.battles 3파티션, bs.dlq)
PYTHONUTF8=1 python producer.py --in data/raw --rate 10        # 전송 (한글 깨짐 방지 PYTHONUTF8)
MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic bs.battles --from-beginning --max-messages 5   # 수신 확인
```

- 토큰 없이 테스트: `python make_sample.py --players 50 --out data/sample` 후 `--in data/sample`로 실행.
- **Windows/Git Bash 주의 (실제 겪은 이슈)**:
  - `docker compose exec`에 `/opt/...` 경로 쓸 때 → **`MSYS_NO_PATHCONV=1`** 필요 (Git Bash 경로 자동변환 방지)
  - 한글 출력 깨지면 → **`PYTHONUTF8=1`**
  - `&`는 PowerShell 문법, Git Bash에선 안 씀
  - `403 Forbidden` = 토큰 등록 IP와 현재 IP 불일치 → developer 포털서 현재 IP로 키 재발급

## 4. Git 워크플로우 (중요 — 이 규칙대로)

- **push 전 항상**: `git fetch origin` → `git rebase origin/main` → `git push origin main`.
  (원격에 user가 웹에서 올린 커밋이 있을 수 있어 바로 push하면 rejected 남)
- **절대 커밋하지 말 것**: `.env`(토큰), `data/`(실데이터), `*.zip`(제출물), `*_capture*.png`(증빙 스크린샷). 모두 `projects/brawlstars-anomaly-pipeline/.gitignore`에 등록돼 있음.
- 커밋 전 `git status --short`로 민감 파일 안 섞였는지 확인.
- **커밋 메시지에 Co-Authored-By(Claude) 라인 넣지 말 것** (user 규칙).
- 커밋 메시지는 간결한 한국어 (`feat:` `fix:` `docs:` `refactor:` 접두어).

## 5. 현재 상태 / 다음 할 일

- ✅ 1~3회차 완료: 데이터·구조 설계(README+구조도), Kafka 수집(producer 공통껍데기·콜백·idempotence·lz4·`--rate`, 토픽 생성), 로컬에서 수집→전송(125건/실패0)→수신 검증, GitHub 업로드.
- README 섹션: §1 데이터 / §2 구조도 / §3 플랫폼 ADR / §4 레포 / §5 Kafka 수집 / §6 트러블슈팅 / §7 재시작·멱등성.
- ⬜ **4회차 과제(전처리·저장, 마감 10/29)**: Spark로 `preprocess.py` 탐지 로직 실구현(봇=플레이 간격 분산≈0+단일 브롤러, 부스팅=승률 급등, 승부조작=상대 쌍 반복) → PostgreSQL/Parquet 저장 → 최종 스키마·저장 포맷 문서화.
- ⬜ 실데이터 더 수집(`fetch_battlelogs.py --max-players`), Grafana 대시보드.

## 6. 핵심 설계 개념 (질문·발표 답변 일관성용)

- **Kafka 역할**: 받아서 쌓고 순서대로 전달하는 버스. 수집·변환은 안 함(수집=fetch, JSON 직렬화=producer).
- **공통 껍데기**: `event_id·event_type·event_time·schema_version·source·payload`.
- **event_id = `battleTime + player_tag`** (데이터 기반 고정) → 재실행해도 동일 → 중복 제거 기준.
- **멱등성 2종**: 전송 멱등(`enable_idempotence`, 재시도 중복만) ≠ 저장 멱등(event_id 유일+upsert, 재실행 중복). 최종 중복 제거는 저장 멱등.
- **멈추면**: fetch/producer는 진행위치 기록 없음 → 처음부터 재전송 → event_id로 중복 흡수(유실 X). 소비는 Kafka offset으로 재개.
- **파티션**: 순서는 파티션 안에서만, 개수=병렬 상한. key=player_tag로 계정 순서 보장.
- **팀원/상대 분리**: `teams`(팀 2개)에서 내 팀=`teammates`, 상대 팀=`opponents`. 승부조작=상대 반복, 팀 어뷰징=팀원 반복.
- **커밋 순서 원칙**: 처리→저장→commit(먼저 commit=유실, 나중=중복). consumer는 `enable_auto_commit=False`.

# Data Engineering

데이터 엔지니어링 프로젝트 모음입니다.

## 프로젝트

### [brawlstars-anomaly-pipeline](projects/brawlstars-anomaly-pipeline) · 구현 완료

브롤스타즈 배틀 로그로 봇·대리(트로피 부스팅)·승부조작 계정을 탐지하는 **Kafka · Spark · Airflow 배치 파이프라인**.

- 로컬에서 약 6만 건을 88초에 처리, 멱등·DLQ·장애복구로 **유실·중복 0** 검증
- 셔플 파티션 200→8 튜닝으로 전처리 **11% 단축** (진짜 병목은 중복 11배 재처리로 규명)
- [프로젝트 README](projects/brawlstars-anomaly-pipeline/README.md) · [개선 이력](projects/brawlstars-anomaly-pipeline/개선이력.md)

### [gameguard-data-pipeline](projects/gameguard-data-pipeline) · 설계 단계

게임 이벤트 수집·적재·이상 탐지 파이프라인 설계안 (Kafka · Spark Streaming · MinIO · Airflow, Bronze/Silver/Gold).
현재 초기 설계 문서 단계입니다. → [프로젝트 README](projects/gameguard-data-pipeline/README.md)

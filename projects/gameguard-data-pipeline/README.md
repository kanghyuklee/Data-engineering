# GameGuard Data Pipeline

**Local Game Event Anomaly Detection & Data Engineering Platform**

## 1. 프로젝트 개요

게임 플레이 이벤트를 수집하고, 데이터 레이크에 적재한 뒤, 플레이어의 이상행동 징후를 탐지하는 데이터 엔지니어링 프로젝트입니다.

AWS 관리형 서비스 대신 Docker 기반 로컬 환경을 사용합니다. 데이터 수집부터 정제, 집계, 품질 검증, 장애 복구까지 구현하는 것을 목표로 합니다.

실제 게임 서비스의 부정행위를 판정하는 시스템이 아닌, 시뮬레이션 데이터를 활용한 이상 징후 탐지 프로젝트입니다.

## 2. 기술 스택

| 영역 | 기술 |
| --- | --- |
| 개발 환경 | Python, Docker Compose |
| 원천 데이터 | PostgreSQL, Event Simulator |
| 메시지 처리 | Apache Kafka |
| 데이터 처리 | Apache Spark |
| 객체 스토리지 | MinIO |
| 워크플로 관리 | Apache Airflow |
| 데이터 분석 | DuckDB |
| 테스트 | Pytest, SQL |

## 3. 데이터 파이프라인

Event Simulator → Kafka → Spark Structured Streaming → MinIO

- Bronze: 수집한 원본 이벤트 보관
- Silver: 스키마 검증, 중복 제거, 데이터 정제
- Gold: 플레이어별 행동 집계 및 이상 징후 탐지

Airflow는 주기적 집계, 데이터 품질 검증, 재처리 작업을 담당합니다.

## 4. 이상 징후 탐지

- R01: 짧은 시간 동안 과도한 게임 재화 획득
- R02: 비정상적으로 높은 동일 행동 반복 빈도
- R03: 게임 규칙상 허용된 속도를 초과하는 이동

탐지 기준값은 설정 파일로 관리하며, 탐지 결과에 관측값과 기준값을 함께 저장합니다.

## 5. 데이터 품질 검증

- 필수 필드 및 데이터 타입 검증
- 이벤트 ID 기준 중복 데이터 처리
- 이벤트 발생 시간과 수집 시간 구분
- 지연 이벤트 처리
- 재실행 시 중복 집계 방지
- 원본·정제·격리 데이터 건수 정합성 확인

## 6. 개발 단계

1. Docker 기반 로컬 환경 구축
2. 게임 이벤트 모델 및 시뮬레이터 구현
3. Kafka Producer/Consumer 구현
4. Spark Bronze/Silver 파이프라인 구현
5. Gold 이상 징후 탐지 구현
6. Airflow DAG 및 데이터 품질 검증
7. 장애·재처리 테스트
8. 성능 측정 및 결과 문서화

## 7. 프로젝트 상태

**Status: Planning**

현재 문서는 초기 설계안입니다. 구현, 실행, 테스트 및 성능 측정 결과는 아직 포함하지 않았습니다.

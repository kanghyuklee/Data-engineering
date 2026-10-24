#!/usr/bin/env bash
# Kafka 토픽 생성 스크립트 (docker-compose의 kafka 컨테이너에서 실행)
# 사용: bash create_topics.sh
set -e

# Git Bash(MSYS)가 /opt/... 경로를 Windows 경로로 자동 변환하는 것 방지
export MSYS_NO_PATHCONV=1

EXEC="docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

# bs.battles — 배틀 이벤트 메인 토픽
#   partition 3  : 병렬 처리 상한 (같은 player_tag는 같은 파티션 → 계정별 순서 보장)
#   retention 7일: 재처리 가능 기간 (604800000 ms)
$EXEC --create --if-not-exists --topic bs.battles \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000

# bs.dlq — 처리 실패 메시지 격리 (재처리 여유 위해 14일 보관)
$EXEC --create --if-not-exists --topic bs.dlq \
  --partitions 1 --replication-factor 1 \
  --config retention.ms=1209600000

echo "=== 생성된 토픽 확인 ==="
$EXEC --describe --topic bs.battles

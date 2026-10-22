-- schema.sql
-- PostgreSQL 저장 스키마. docker-compose가 postgres 최초 기동 시 자동 실행한다.
-- 조회/집계가 잦은 '결과·요약'을 여기에 둔다. (원본 배틀은 Parquet)

-- 계정별 탐지 결과
CREATE TABLE IF NOT EXISTS detection_result (
    player_tag      TEXT        PRIMARY KEY,
    battles         INT,
    win_rate        DOUBLE PRECISION,
    max_win_streak  INT,
    avg_interval_s  DOUBLE PRECISION,   -- 배틀 간 평균 간격
    std_interval_s  DOUBLE PRECISION,   -- 간격 분산 (봇이면 0에 가까움)
    trophy_gain     INT,                -- 누적 트로피 증감
    brawler_variety INT,                -- 사용 브롤러 종류 수
    anomaly_score   DOUBLE PRECISION,
    anomaly_reason  TEXT,               -- 'bot' / 'boosting' / 'win_trade' 등
    is_injected     SMALLINT DEFAULT 0, -- 평가용 정답 라벨
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_detection_score
    ON detection_result (anomaly_score DESC);

-- 배틀 요약 (필요 시 bs.battles 원본 일부 적재)
CREATE TABLE IF NOT EXISTS battle_summary (
    event_id     TEXT PRIMARY KEY,      -- battleTime + player_tag, 중복 제거 기준
    battle_id    TEXT,
    player_tag   TEXT,
    ts           TIMESTAMPTZ,
    mode         TEXT,
    result       TEXT,
    trophy_change INT,
    is_injected  SMALLINT DEFAULT 0
);

-- TODO: 승부조작 탐지용 (player_tag, opponent_tag) 대전 빈도 테이블도 필요해지면 추가

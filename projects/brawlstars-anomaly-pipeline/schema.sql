-- schema.sql
-- PostgreSQL 저장 스키마 (docker-compose가 postgres 최초 기동 시 자동 실행).
-- 조회/집계가 잦은 '결과'를 여기에 둔다. 정제된 '원본 이벤트'는 Parquet(data/parquet/battles)에 저장.
--
-- 참고: preprocess.py(Spark)가 detection_result를 mode("overwrite")로 다시 써서
--       실행 시 이 테이블을 DF 스키마에 맞춰 재생성한다. 아래 정의는 '최종 스키마 문서'이자
--       Spark 실행 전 Grafana 등에서 참조할 기본 틀이다.

-- 계정별 탐지 결과 (preprocess.py의 집계 산출물)
CREATE TABLE IF NOT EXISTS detection_result (
    player_tag      TEXT        PRIMARY KEY,      -- 계정 (고유 키)
    battles         INT,                          -- 집계 대상 배틀 수
    win_rate        DOUBLE PRECISION,             -- 승률 (0~1)
    trophy_gain     INT,                          -- 누적 트로피 증감
    brawler_variety INT,                          -- 사용 브롤러 종류 수 (1이면 봇 의심)
    anomaly_score   DOUBLE PRECISION,             -- 이상 점수 (0~1)
    anomaly_reason  TEXT,                          -- 'bot' / 'boosting' / 'normal'
    is_injected     SMALLINT,                     -- 평가용 정답 라벨(가짜 이상 계정=1)
    updated_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_detection_score
    ON detection_result (anomaly_score DESC);     -- 의심 계정 상위 조회용

-- TODO(확장): 승부조작 탐지용 (player_tag, opponent_tag) 대전 빈도 테이블
--             opponents 배열을 explode 해서 집계 → 상대 반복 횟수 기반

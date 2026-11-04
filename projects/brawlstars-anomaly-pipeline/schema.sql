-- schema.sql
-- PostgreSQL 저장 스키마 (docker-compose가 postgres 최초 기동 시 자동 실행).
-- 조회/집계가 잦은 '결과'를 여기에 둔다. 정제된 '원본 이벤트'는 Parquet(data/parquet/battles)에 저장.
--
-- 참고: preprocess.py(Spark)는 이번 실행분을 detection_result_staging 에 적재하고,
--       upsert_detection.py 가 detection_result 로 ON CONFLICT(player_tag) DO UPDATE
--       병합한다. → 이번 배치에 없는 계정은 유지되고(날짜 누적), 아래 PK·인덱스도
--       보존된다 (detection_result 는 Spark가 직접 쓰지 않아 재생성되지 않음).
--       그래서 이 스키마는 '문서'이자 '실제 운영 스키마'다.
--       detection_result_staging 은 Spark overwrite가 매 실행 DROP+CREATE 하는 임시
--       테이블이라 여기 정의하지 않는다(스키마는 scored DataFrame에서 자동 추론).

-- 계정별 탐지 결과 (preprocess.py의 집계 산출물)
CREATE TABLE IF NOT EXISTS detection_result (
    player_tag      TEXT        PRIMARY KEY,      -- 계정 (고유 키)
    battles         INT,                          -- 집계 대상 배틀 수
    win_rate        DOUBLE PRECISION,             -- 승률 (0~1)
    trophy_gain     INT,                          -- 누적 트로피 증감
    brawler_variety INT,                          -- 사용 브롤러 종류 수 (1이면 봇 의심)
    anomaly_score   DOUBLE PRECISION,             -- 룰 기반 이상 점수 (0~1)
    anomaly_reason  TEXT,                          -- 'bot' / 'boosting' / 'normal'
    ml_score        DOUBLE PRECISION,             -- ML(IsolationForest) 이상도 (0~1, ml_detect.py가 채움)
    max_opp_repeat  INT,                          -- 한 상대를 만난 최대 횟수 (승부조작 신호, match_fixing.py가 채움)
    final_score     DOUBLE PRECISION,             -- 룰·ML·승부조작 통합 점수 MAX (combine_scores.py가 채움)
    flagged_by      TEXT,                          -- 걸린 신호 출처 'rule,ml' 등 (combine_scores.py)
    is_injected     SMALLINT,                     -- 평가용 정답 라벨(가짜 이상 계정=1)
    updated_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_detection_score
    ON detection_result (anomaly_score DESC);     -- 의심 계정 상위 조회용

-- TODO(확장): 승부조작 탐지용 (player_tag, opponent_tag) 대전 빈도 테이블
--             opponents 배열을 explode 해서 집계 → 상대 반복 횟수 기반

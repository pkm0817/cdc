-- ============================================================================
-- Target DB 스키마
--
-- car      : source 와 완전히 동일한 스키마 → cdc-service 가 1:1 그대로 복제
--            (id 는 source 가 발번한 값을 받으므로 BIGSERIAL 이 아니라 BIGINT)
--
-- computer : source 와 "다른" 스키마 → cdc-service 가 변환해서 적재
--            source(brand, model, cpu, ram_gb, price_usd)
--              → full_name  = brand || ' ' || model
--              → spec       = cpu || ' / ' || ram_gb || 'GB'
--              → price_krw  = price_usd × 1350 (데모용 고정 환율)
--            + 동기화 정확성을 위한 부가 컬럼 3개:
--              deleted    : 소프트 삭제 (물리 삭제하면 늦게 온 UPDATE 가 행을 되살린다)
--              source_lsn : 순서 역전 방어 — 더 오래된 이벤트가 늦게 도착해도 덮어쓰지 않음
--              synced_at  : 마지막 동기화 시각 (지연 관측용)
-- ============================================================================

CREATE TABLE car (
    id         BIGINT       PRIMARY KEY,
    name       TEXT         NOT NULL,
    brand      TEXT         NOT NULL,
    price      NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL,
    updated_at TIMESTAMPTZ  NOT NULL
);

CREATE TABLE computer (
    id         BIGINT       PRIMARY KEY,
    full_name  TEXT         NOT NULL,
    spec       TEXT         NOT NULL,
    price_krw  NUMERIC(14,0) NOT NULL,
    deleted    BOOLEAN      NOT NULL DEFAULT FALSE,
    source_lsn BIGINT       NOT NULL,
    synced_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE grade (
    id            BIGINT       PRIMARY KEY,
    code          TEXT         NOT NULL,
    name          TEXT         NOT NULL,
    discount_rate NUMERIC(5,2) NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
    source_lsn    BIGINT       NOT NULL,
    synced_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- email 에 UNIQUE 를 걸지 않는다. 소프트 삭제로 행이 남은 상태에서
-- 같은 이메일이 새 id 로 다시 들어오면 적재가 막히기 때문이다.
-- 유일성은 source 가 이미 보장하고 있다.
CREATE TABLE member (
    id         BIGINT       PRIMARY KEY,
    email      TEXT         NOT NULL,
    name       TEXT         NOT NULL,
    grade_id   BIGINT       NOT NULL,   -- FK 제약 없음 (위 주석 참고)
    point      INT          NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL,
    updated_at TIMESTAMPTZ  NOT NULL,
    deleted    BOOLEAN      NOT NULL DEFAULT FALSE,
    source_lsn BIGINT       NOT NULL,
    synced_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX member_grade_id_idx ON member (grade_id);

-- ============================================================================
-- 파이프라인 진행 지점
--
-- Debezium 의 오프셋 파일과 별개로 "우리가 어디까지 처리했는지"를 수신 측에 남긴다.
-- 오프셋 파일은 Provider 볼륨에 있어서 볼륨이 사라지면 같이 없어지는데,
-- 그러면 "처음부터 다시 읽는 것"과 "구간을 건너뛴 것"을 구분할 수 없다.
-- 이 표가 남아 있으면 기동 시 슬롯의 restart_lsn 과 대조해 유실을 탐지할 수 있다.
-- ============================================================================
CREATE TABLE cdc_checkpoint (
    pipeline         TEXT        PRIMARY KEY,
    last_applied_lsn BIGINT      NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL
);

-- ============================================================================
-- 반영하지 못한 이벤트 (DLQ)
--
-- 여기 들어간 것은 유실이 아니라 "추적되는 미반영"이다.
-- payload 만으로 원래 이벤트를 재구성할 수 있어야 재처리가 가능하다.
--
-- 수신 측 DB 에 두는 이유: 별도 저장소면 "적용도 실패, 격리 기록도 실패"가
-- 진짜 유실이 된다. 같은 DB 면 DB 가 죽었을 때는 재시도 경로로 빠지므로
-- 어느 쪽이든 유실 경로가 생기지 않는다.
--
-- status: PENDING(격리됨) · RETRY_REQUESTED(재처리 대상으로 표시) ·
--         RESOLVED(재처리 성공) · DISCARDED(버리기로 결정)
-- ============================================================================
CREATE TABLE cdc_dead_letter (
    id                BIGSERIAL   PRIMARY KEY,
    pipeline          TEXT        NOT NULL,
    source_table      TEXT        NOT NULL,
    op                TEXT        NOT NULL,
    source_lsn        BIGINT      NOT NULL,
    payload           TEXT        NOT NULL,
    failure_type      TEXT        NOT NULL,
    failure_sql_state TEXT,
    failure_message   TEXT        NOT NULL,
    attempts          INT         NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'PENDING',
    first_failed_at   TIMESTAMPTZ NOT NULL,
    last_failed_at    TIMESTAMPTZ NOT NULL
);

-- 미처리 건 조회가 경보의 기준이므로 인덱스를 건다.
CREATE INDEX cdc_dead_letter_status_idx ON cdc_dead_letter (pipeline, status, id);

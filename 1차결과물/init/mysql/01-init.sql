-- ─────────────────────────────────────────────────────────────────────
-- Target 스키마: MySQL user
-- 업무 컬럼은 members 와 이름이 전부 다르고, 동기화 정확성을 위한
-- 부가 컬럼이 둘 붙는다:
--   deleted    — 소프트 삭제. 물리 삭제하면 늦게 온 UPDATE 가 행을 되살린다
--   source_lsn — 결정적 시퀀싱. 순서 역전된 이벤트를 무시하기 위한 기준
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE `user` (
    id          BIGINT       NOT NULL PRIMARY KEY,   -- ← member_id
    name        VARCHAR(100) NOT NULL,               -- ← full_name
    email       VARCHAR(200) NOT NULL,               -- ← email_address
    user_status VARCHAR(32)  NOT NULL,               -- ← status
    deleted     BOOLEAN      NOT NULL DEFAULT FALSE,
    source_lsn  BIGINT       NOT NULL,
    synced_at   DATETIME(3)  NOT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

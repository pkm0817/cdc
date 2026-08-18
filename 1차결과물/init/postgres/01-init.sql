-- ─────────────────────────────────────────────────────────────────────
-- Source 스키마: PostgreSQL members
-- 대상(MySQL user)과 컬럼명이 전부 다르다 — 이것이 애플리케이션 컨슈머를
-- 직접 만드는 유일한 정당화 사유. 스키마가 같다면 JDBC Sink Connector로 충분.
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE members (
    member_id     BIGSERIAL PRIMARY KEY,
    full_name     VARCHAR(100) NOT NULL,
    email_address VARCHAR(200) NOT NULL,
    status        VARCHAR(32)  NOT NULL DEFAULT 'ACTIVE',
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ⭐ MySQL의 binlog_row_image = FULL 에 해당.
-- 없으면 UPDATE/DELETE 이벤트의 before 가 PK만 담긴다.
ALTER TABLE members REPLICA IDENTITY FULL;

-- ── 최소 권한 CDC 계정 ────────────────────────────────────────────────
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'cdc_pw';
GRANT USAGE  ON SCHEMA public TO cdc_user;
GRANT SELECT ON members       TO cdc_user;   -- snapshot 용
GRANT CREATE ON DATABASE memberdb TO cdc_user;  -- publication 소유에 필요

CREATE PUBLICATION member_pub FOR TABLE members;
ALTER PUBLICATION member_pub OWNER TO cdc_user;

-- ── snapshot 이 읽어갈 초기 데이터 (op: r 로 흘러간다) ────────────────
INSERT INTO members (full_name, email_address, status) VALUES
    ('홍길동', 'gildong@example.com', 'ACTIVE'),
    ('김철수', 'chulsoo@example.com', 'ACTIVE');

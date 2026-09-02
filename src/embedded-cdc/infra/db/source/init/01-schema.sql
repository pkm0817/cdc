-- ============================================================================
-- Source DB 스키마
--   car      : target 과 스키마가 "같은" 경우
--   computer : target 과 스키마가 "다른" 경우 (cdc-service 가 매핑/변환)
--   grade    : member 가 참조하는 부모 테이블
--   member   : grade 를 FK 로 참조하는 자식 테이블 — "관계가 있는" 경우
--
--   source 에는 FK 를 두지만 target 에는 두지 않는다. 근거는 target 스키마 주석 참고.
-- ============================================================================

CREATE TABLE car (
    id         BIGSERIAL    PRIMARY KEY,
    name       TEXT         NOT NULL,
    brand      TEXT         NOT NULL,
    price      NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE computer (
    id         BIGSERIAL    PRIMARY KEY,
    brand      TEXT         NOT NULL,
    model      TEXT         NOT NULL,
    cpu        TEXT         NOT NULL,
    ram_gb     INT          NOT NULL,
    price_usd  NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- UPDATE/DELETE 이벤트의 before 이미지에 전체 컬럼을 담는다.
-- 기본값(DEFAULT)이면 before 에 PK 만 담겨 소프트 삭제 시 정보가 부족하다.
-- (MySQL 의 binlog_row_image = FULL 에 해당)
ALTER TABLE car      REPLICA IDENTITY FULL;
ALTER TABLE computer REPLICA IDENTITY FULL;

-- ============================================================================
-- 관계가 있는 테이블 쌍
--   member.grade_id -> grade.id
-- FK 는 source 에만 둔다. 수신 측에 같은 제약을 걸면 이벤트 적용 순서가
-- 어긋나는 순간(DLQ 격리, 전량 재적재) 연쇄 실패가 난다.
-- ============================================================================
CREATE TABLE grade (
    id            BIGSERIAL     PRIMARY KEY,
    code          TEXT          NOT NULL UNIQUE,
    name          TEXT          NOT NULL,
    discount_rate NUMERIC(5,2)  NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE member (
    id         BIGSERIAL     PRIMARY KEY,
    email      TEXT          NOT NULL UNIQUE,
    name       TEXT          NOT NULL,
    grade_id   BIGINT        NOT NULL REFERENCES grade(id),
    point      INT           NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX member_grade_id_idx ON member (grade_id);

ALTER TABLE grade  REPLICA IDENTITY FULL;
ALTER TABLE member REPLICA IDENTITY FULL;

-- ============================================================================
-- CDC 전용 최소 권한 계정
--   REPLICATION : WAL 스트림 수신 (replication slot 생성 포함)
--   SELECT      : 최초 snapshot 시 테이블 읽기
-- ============================================================================
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'cdc_pass';
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON car, computer, grade, member TO cdc_user;

-- 캡처 대상 테이블 집합. Debezium 설정의 publication.name 과 일치해야 한다.
CREATE PUBLICATION embedded_cdc_pub FOR TABLE car, computer, grade, member;
ALTER PUBLICATION embedded_cdc_pub OWNER TO cdc_user;

-- ============================================================================
-- 초기 데이터 — 최초 기동 시 snapshot(op: r) 으로 target 에 복제되는지 확인용
-- ============================================================================
INSERT INTO car (name, brand, price) VALUES
    ('Avante',  'Hyundai', 21500000.00),
    ('K5',      'Kia',     28900000.00),
    ('Model 3', 'Tesla',   52900000.00);

INSERT INTO computer (brand, model, cpu, ram_gb, price_usd) VALUES
    ('Apple',  'MacBook Pro 14', 'M4 Pro',          24, 1999.00),
    ('Lenovo', 'ThinkPad X1',    'Core Ultra 7',    32, 1650.00),
    ('Dell',   'XPS 13',         'Core Ultra 5',    16, 1199.00);

-- grade 를 먼저 넣어야 member 의 FK 가 성립한다.
INSERT INTO grade (code, name, discount_rate) VALUES
    ('BRONZE',   '브론즈',     0.00),
    ('SILVER',   '실버',       3.00),
    ('GOLD',     '골드',       5.00),
    ('PLATINUM', '플래티넘',   7.00),
    ('VIP',      'VIP',       10.00);

-- grade_id 를 id 로 하드코딩하지 않고 code 로 조회한다 —
-- 시드가 늘거나 순서가 바뀌어도 깨지지 않는다.
INSERT INTO member (email, name, grade_id, point)
SELECT v.email, v.name, g.id, v.point
FROM (VALUES
    ('kim@example.com',  '김철수', 'BRONZE',   1200),
    ('lee@example.com',  '이영희', 'SILVER',   5400),
    ('park@example.com', '박민수', 'GOLD',    18000),
    ('choi@example.com', '최지현', 'PLATINUM',42000),
    ('jung@example.com', '정다은', 'VIP',    130000)
) AS v(email, name, grade_code, point)
JOIN grade g ON g.code = v.grade_code;

-- ============================================================================
-- heartbeat 전용 테이블
--
-- 관심 테이블에 변경이 없으면 슬롯이 전혀 전진하지 않아 WAL 이 계속 쌓인다.
-- Debezium 의 heartbeat.action.query 가 여기에 쓰기를 만들고, 그 변경이
-- publication 을 타고 나가면서 confirmed_flush_lsn 이 전진한다.
--
-- V6 측정: heartbeat.interval.ms 만 켜면 전진 0 bytes, action.query 를 함께
-- 걸면 33,440 bytes. 즉 "주기"가 아니라 "publication 에 든 테이블에 실제 쓰기"가
-- 슬롯을 미는 것이다. 그래서 설정 2줄이 아니라 전용 테이블이 딸려온다.
--
-- publication 과 table.include.list 에 둘 다 넣는다.
-- publication 에만 넣으면 heartbeat 쓰기가 슬롯만 밀고 파이프라인의 진행 지점
-- (cdc_checkpoint)은 멈춰 있게 되어, 다음 기동에서 SlotContinuityGuard 가 그 간격을
-- 캡처 갭으로 읽고 기동을 거부한다 (유휴 36분에 1.1MB 어긋나 실제로 발생했다).
-- include.list 에 넣으면 이벤트가 파이프라인까지 오는데, 매핑된 핸들러가 없어
-- 적용은 건너뛰고 배치의 최대 LSN 만 체크포인트로 남는다 —
-- 그래서 수신 측에 이 테이블을 만들 필요는 여전히 없다.
--
-- UPSERT 로 한 행만 유지한다. append 로 두면 heartbeat 자체가 테이블을 키운다.
-- ============================================================================
CREATE TABLE cdc_heartbeat (
    pipeline TEXT        PRIMARY KEY,
    beat_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER PUBLICATION embedded_cdc_pub ADD TABLE cdc_heartbeat;

-- heartbeat 쿼리는 커넥터 자신의 커넥션(cdc_user)으로 실행된다.
-- 이 권한이 없으면 쿼리가 조용히 실패하고 슬롯은 여전히 안 움직인다 —
-- 설정을 켠 뒤에도 증상이 그대로라 원인을 엉뚱한 데서 찾게 된다.
GRANT SELECT, INSERT, UPDATE ON cdc_heartbeat TO cdc_user;

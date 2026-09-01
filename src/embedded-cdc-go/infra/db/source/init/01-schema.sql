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

-- UPDATE/DELETE 이벤트의 old 이미지에 전체 컬럼을 담는다.
-- 기본값(DEFAULT)이면 old 에 PK 만 담겨 소프트 삭제 시 정보가 부족하다.
-- go-pq-cdc 는 UPDATE 에서 TOAST 로 빠진 컬럼을 old 이미지 값으로 메워 주므로,
-- FULL 은 "삭제 정보"뿐 아니라 "TOAST 복원"의 전제이기도 하다.
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
-- heartbeat 테이블
--
-- 관심 테이블에 변경이 없으면 슬롯의 confirmed_flush_lsn 이 전진하지 않고,
-- 다른 테이블이 만든 WAL 까지 슬롯이 계속 붙잡는다(V6 에서 실측).
-- go-pq-cdc 는 이 표에 주기적으로 써서 스스로 WAL 을 만들고 슬롯을 밀어 준다.
-- 반드시 publication 에 들어 있어야 하며(라이브러리가 기동 시 검증한다),
-- 이 표의 이벤트는 파이프라인에 전달되기 전에 걸러진다.
-- ============================================================================
-- go-pq-cdc 가 만드는 것과 같은 모양으로 미리 만들어 둔다. 라이브러리는 존재 여부를
-- 먼저 확인하므로, 여기서 만들어 두면 그대로 쓰면서 publication 에도 넣어 둘 수 있다
-- (publication 은 이 파일에서 한 번에 정의되는데, 표가 없으면 넣을 수 없다).
CREATE TABLE cdc_heartbeat (
    id             INTEGER     PRIMARY KEY DEFAULT 1,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cdc_heartbeat_single_row CHECK (id = 1)
);
INSERT INTO cdc_heartbeat (id) VALUES (1);

-- ============================================================================
-- CDC 전용 최소 권한 계정
--   REPLICATION : WAL 스트림 수신 (replication slot 생성 포함)
--   SELECT      : 최초 snapshot 시 테이블 읽기
--
-- Java(Debezium) 판과 달리 쓰기 권한이 필요하다. 이유가 둘 있다.
--   1. heartbeat  : cdc_heartbeat 에 직접 INSERT/UPDATE 한다
--   2. snapshot   : go-pq-cdc 는 스냅샷 진행 상황(cdc_snapshot_job,
--                   cdc_snapshot_chunks)을 원천 DB 에 만들어 관리한다.
--                   여러 인스턴스가 청크를 나눠 잡을 수 있게 하려는 설계다.
-- 원천 DB 에 파이프라인 살림살이가 생긴다는 뜻이므로, 원천을 건드릴 수 없는
-- 환경이라면 snapshot.enabled=false 로 끄고 초기 적재를 따로 해야 한다.
-- ============================================================================
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'cdc_pass';
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON car, computer, grade, member TO cdc_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON cdc_heartbeat TO cdc_user;
GRANT CREATE ON SCHEMA public TO cdc_user;   -- snapshot 메타데이터 테이블 생성용

-- 캡처 대상 테이블 집합. go-pq-cdc 설정의 publication.name 과 일치해야 한다.
-- Java 판과 이름을 달리해, 실수로 같은 원천을 가리켜도 서로를 덮어쓰지 않게 한다.
CREATE PUBLICATION embedded_cdc_go_pub
    FOR TABLE car, computer, grade, member, cdc_heartbeat;
ALTER PUBLICATION embedded_cdc_go_pub OWNER TO cdc_user;

-- ============================================================================
-- 초기 데이터 — 최초 기동 시 snapshot 으로 target 에 복제되는지 확인용
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

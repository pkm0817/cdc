-- ============================================================================
-- Source DB 스키마 (cdc-custom)
--
-- 업무 테이블 4개는 embedded-cdc / embedded-cdc-go 와 **완전히 동일**하다.
-- 세 방식을 같은 데이터·같은 부하로 비교하기 위해서다.
--
-- 다른 점은 두 가지뿐이다.
--   1) REPLICA IDENTITY FULL 이 없다. WAL 을 읽지 않으므로 필요 없다.
--      (CDC 판에서는 이것 때문에 UPDATE 1건이 변경 전+후 이미지를 함께 실어
--       행당 1KB 가까이 WAL 을 썼다. 여기서는 그 비용이 통째로 사라진다)
--   2) sync_outbox 와 문장 단위 트리거가 붙는다. 아래 참고.
--
-- wal_level 도 logical 일 필요가 없다. 복제 슬롯을 만들지 않으므로
-- "슬롯이 WAL 을 붙잡아 디스크가 고갈되는" 위험 자체가 없다.
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

-- ============================================================================
-- 변경 로그 (outbox)
--
-- "무엇이 어떻게 바뀌었는지"가 아니라 **"어느 행이 손댔는지"만** 적는다.
-- 값은 반영 시점에 소스에서 다시 읽는다. 두 DB 가 동기화되어 있다는 전제 위에서
-- 최종 상태만 맞추면 되기 때문이다.
--
-- 이 선택이 만드는 차이:
--   - 기록량   : 행당 약 30바이트. CDC 의 WAL(UPDATE 기준 행당 1KB)의 3% 수준.
--   - 중복 접기: 같은 행이 100번 바뀌어도 반영은 1번. CDC 는 100번 모두 적용한다.
--   - 순서 문제: 항상 "현재 값"을 읽으므로 순서 역전으로 옛 값이 덮어쓸 일이 없다.
--
-- 대가도 분명하다. **중간 상태가 남지 않는다.** 변경 이력·감사에는 쓸 수 없고,
-- "언제 무엇이 무엇으로 바뀌었나"를 물으면 답할 수 없다.
--
-- seq 는 BIGSERIAL 이다. 커밋 순서와 seq 순서가 어긋날 수 있지만(먼저 번호를 받은
-- 트랜잭션이 나중에 커밋될 수 있다) 문제되지 않는다 — 반영은 항상 현재 값을 읽고,
-- 놓친 행은 다음 배치에서 다시 잡히기 때문이다. 체크포인트를 "커밋되지 않은 seq"
-- 너머로 밀지 않도록, 워커는 읽은 배치의 최대 seq 까지만 전진한다.
-- ============================================================================
CREATE TABLE sync_outbox (
    seq        BIGSERIAL   PRIMARY KEY,
    table_name TEXT        NOT NULL,
    row_id     BIGINT      NOT NULL,
    op         CHAR(1)     NOT NULL CHECK (op IN ('c', 'u', 'd')),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ============================================================================
-- 포착 트리거
--
-- **문장 단위(FOR EACH STATEMENT)** 트리거에 전이 테이블(REFERENCING)을 붙였다.
-- 행 단위 트리거였다면 30만 행 UPDATE 에서 트리거가 30만 번 호출돼, 재려는 대상이
-- 아니라 부하 생성 쪽이 병목이 된다. 문장당 한 번 호출해서 INSERT ... SELECT 로
-- 바뀐 PK 를 한꺼번에 밀어 넣으면 30만 행이 INSERT 한 문장으로 끝난다.
--
-- 트리거는 원 트랜잭션 안에서 돈다. 즉 outbox 기록이 실패하면 업무 트랜잭션도
-- 함께 실패한다 — 변경만 반영되고 기록이 누락되는 상태가 생기지 않는다.
-- ============================================================================
CREATE OR REPLACE FUNCTION sync_outbox_capture() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO sync_outbox (table_name, row_id, op)
        SELECT TG_TABLE_NAME, o.id, 'd' FROM old_rows o;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO sync_outbox (table_name, row_id, op)
        SELECT TG_TABLE_NAME, n.id, 'u' FROM new_rows n;
    ELSE
        INSERT INTO sync_outbox (table_name, row_id, op)
        SELECT TG_TABLE_NAME, n.id, 'c' FROM new_rows n;
    END IF;
    RETURN NULL;  -- AFTER STATEMENT 트리거의 반환값은 무시된다
END;
$$ LANGUAGE plpgsql;

-- 4개 테이블 x 3개 연산. TRUNCATE 는 전이 테이블을 지원하지 않으므로 걸지 않는다
-- (실습에서 TRUNCATE 를 쓰면 outbox 에 아무것도 남지 않아 타깃이 그대로 남는다.
--  전량 재적재로 맞춰야 한다 — CDC 판에서 TRUNCATE 이벤트를 무시하는 것과 같은 처지다).
DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['car', 'computer', 'grade', 'member'] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I_sync_ins AFTER INSERT ON %I
               REFERENCING NEW TABLE AS new_rows
               FOR EACH STATEMENT EXECUTE FUNCTION sync_outbox_capture()', t, t);
        EXECUTE format(
            'CREATE TRIGGER %I_sync_upd AFTER UPDATE ON %I
               REFERENCING NEW TABLE AS new_rows
               FOR EACH STATEMENT EXECUTE FUNCTION sync_outbox_capture()', t, t);
        EXECUTE format(
            'CREATE TRIGGER %I_sync_del AFTER DELETE ON %I
               REFERENCING OLD TABLE AS old_rows
               FOR EACH STATEMENT EXECUTE FUNCTION sync_outbox_capture()', t, t);
    END LOOP;
END;
$$;

-- ============================================================================
-- 초기 데이터 — CDC 판과 동일하게 맞춘다
-- ============================================================================
INSERT INTO grade (code, name, discount_rate) VALUES
    ('BRONZE', '브론즈', 0),
    ('SILVER', '실버',   3),
    ('GOLD',   '골드',   5),
    ('VIP',    'VIP',    10),
    ('VVIP',   'VVIP',   15);

INSERT INTO car (name, brand, price) VALUES
    ('Avante',  'Hyundai', 25000000),
    ('Model 3', 'Tesla',   62000000),
    ('K5',      'Kia',     31000000);

INSERT INTO computer (brand, model, cpu, ram_gb, price_usd) VALUES
    ('Apple',  'MacBook Pro 14', 'M4 Pro',       24, 2499.00),
    ('Lenovo', 'ThinkPad X1',    'Core Ultra 7', 32, 1899.00);

INSERT INTO member (email, name, grade_id, point) VALUES
    ('a@example.com', '김하나', 1, 100),
    ('b@example.com', '이두리', 3, 5000);

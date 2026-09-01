-- ============================================================================
-- 랜덤 대량 시드 — CDC 부하/정합성 확인용
--
--   podman exec -i emb-cdc-source-pg psql -U postgres -d sourcedb \
--       -v ON_ERROR_STOP=1 -f - < scripts/seed-bulk.sql
--
-- 여러 번 실행하면 그때마다 테이블당 1000 건씩 누적된다.
-- 건수는 각 블록의 generate_series(1, 1000) 한 자리만 고치면 된다.
--
-- 주의 1) random() 은 반드시 gen CTE 에서 "행마다" 뽑아 컬럼으로 들고 다닌다.
--         LATERAL 안에서 직접 호출하면, 그 서브쿼리가 바깥 행(i)을 참조하지 않는 한
--         플래너가 상수로 보고 문장 전체에서 한 번만 계산한다 → 1000 건이 전부 같은 값.
-- 주의 2) INTERVAL '1 day' 대신 make_interval() 을 쓴다. 셸을 거치며 따옴표가
--         벗겨지면 INTERVAL 이 컬럼 이름으로 해석돼 깨진다.
-- ============================================================================

-- ── member : grade 를 FK 로 참조하는 자식 테이블 ────────────────────────────
WITH cfg AS (
    SELECT
        ARRAY['김','이','박','최','정','강','조','윤','장','임',
              '한','오','서','신','권','황','안','송','류','전']            AS surnames,
        ARRAY['철수','영희','민수','지현','다은','서준','하윤','도윤','시우','주원',
              '예린','수빈','지호','채원','민재','유진','건우','소율','태현','나윤'] AS givens,
        -- id 를 하드코딩하지 않는다. discount_rate 순 = 등급 순이라 포인트 스케일에 그대로 쓴다.
        (SELECT array_agg(id ORDER BY discount_rate) FROM grade)            AS grade_ids,
        -- 재실행해도 이메일이 겹치지 않게 현재 최대 id 를 오프셋으로 쓴다.
        (SELECT coalesce(max(id), 0) FROM member)                          AS id_base
),
gen AS (
    SELECT i,
           random() AS r_sur, random() AS r_giv, random() AS r_tier,
           random() AS r_point, random() AS r_day, random() AS r_sec
    FROM generate_series(1, 1000) AS i
)
INSERT INTO member (email, name, grade_id, point, created_at, updated_at)
SELECT
    format('member%s@example.com', cfg.id_base + gen.i),
    cfg.surnames[1 + floor(gen.r_sur * array_length(cfg.surnames, 1))::int]
      || cfg.givens[1 + floor(gen.r_giv * array_length(cfg.givens, 1))::int],
    cfg.grade_ids[t.tier],
    (gen.r_point * t.tier * 30000)::int,          -- 상위 등급일수록 포인트가 크다
    c.created,
    c.created + make_interval(secs => (gen.r_sec * 86400)::int)
FROM gen
CROSS JOIN cfg
CROSS JOIN LATERAL (SELECT 1 + floor(gen.r_tier * array_length(cfg.grade_ids, 1))::int AS tier) t
CROSS JOIN LATERAL (SELECT now() - make_interval(days => (gen.r_day * 90)::int) AS created) c
ON CONFLICT (email) DO NOTHING;

-- ── car : target 과 스키마가 같은 테이블 ────────────────────────────────────
WITH cfg AS (
    SELECT ARRAY['Hyundai','Kia','Tesla','BMW','Benz','Audi','Volvo','Toyota'] AS brands,
           ARRAY['Avante','Sonata','K5','Sorento','Model 3','Model Y',
                 'X3','E-Class','A6','XC60','Camry']                           AS models
),
gen AS (
    SELECT random() AS r_brand, random() AS r_model, random() AS r_year, random() AS r_price
    FROM generate_series(1, 1000)
)
INSERT INTO car (name, brand, price)
SELECT
    cfg.models[1 + floor(gen.r_model * array_length(cfg.models, 1))::int]
      || ' ' || (2018 + floor(gen.r_year * 8))::text,
    cfg.brands[1 + floor(gen.r_brand * array_length(cfg.brands, 1))::int],
    (15000000 + gen.r_price * 85000000)::numeric(12,2)
FROM gen CROSS JOIN cfg;

-- ── computer : target 과 스키마가 달라 cdc-service 가 매핑/변환하는 테이블 ──
WITH cfg AS (
    SELECT ARRAY['Apple','Lenovo','Dell','HP','ASUS','Samsung','LG']            AS brands,
           ARRAY['MacBook Pro','ThinkPad X1','XPS','Spectre','ROG Zephyrus',
                 'Galaxy Book','Gram']                                          AS models,
           ARRAY['M4 Pro','M4 Max','Core Ultra 5','Core Ultra 7','Core Ultra 9',
                 'Ryzen 7','Ryzen 9']                                           AS cpus,
           ARRAY[8, 16, 24, 32, 48, 64]                                         AS rams
),
gen AS (
    SELECT random() AS r_brand, random() AS r_model, random() AS r_sku,
           random() AS r_cpu, random() AS r_ram, random() AS r_price
    FROM generate_series(1, 1000)
)
INSERT INTO computer (brand, model, cpu, ram_gb, price_usd)
SELECT
    cfg.brands[1 + floor(gen.r_brand * array_length(cfg.brands, 1))::int],
    cfg.models[1 + floor(gen.r_model * array_length(cfg.models, 1))::int]
      || '-' || (100 + floor(gen.r_sku * 900))::text,
    cfg.cpus[1 + floor(gen.r_cpu * array_length(cfg.cpus, 1))::int],
    cfg.rams[1 + floor(gen.r_ram * array_length(cfg.rams, 1))::int],
    (699 + gen.r_price * 3300)::numeric(10,2)
FROM gen CROSS JOIN cfg;

-- ============================================================================
-- 대량 UPDATE — CDC 반영 시간 측정용 (seed-bulk.sql 의 짝)
--
--   podman exec -i emb-cdc-source-pg psql -U postgres -d sourcedb \
--       -v ON_ERROR_STOP=1 -v cnt=1000 -f - < scripts/update-bulk.sql
--
-- 세 테이블(member/car/computer)에서 id 오름차순으로 cnt 행씩 골라 값 하나를 흔든다.
-- 테이블당 cnt 행이므로 한 번 실행에 3 x cnt 개의 UPDATE 이벤트가 흐른다.
--
-- 주의 1) 대상 선정에 ORDER BY random() 을 쓰지 않는다. 수백만 행에서 매번 풀스캔+정렬이
--         되어, 재려는 CDC 가 아니라 부하 생성 쪽에서 시간을 먹는다.
--         PK 인덱스를 따라 앞에서부터 cnt 행만 훑는다.
-- 주의 2) 매번 같은 행(id 앞에서부터)을 고른다. 배치 크기를 키워가며 여러 번 돌려도
--         대상 행 분포가 같아 비교 조건이 유지된다.
-- 주의 3) 세 테이블 모두 REPLICA IDENTITY FULL 이다. UPDATE 는 변경 전 이미지까지
--         WAL 에 실리므로, 같은 건수의 INSERT 보다 WAL 이 크고 반영도 느릴 수 있다.
--         이 차이를 보는 것이 이 측정의 목적이다.
-- ============================================================================

-- 호출 쪽에서 cnt 를 주지 않았을 때만 기본값을 채운다.
\if :{?cnt}
\else
\set cnt 1000
\endif

-- ── member : point 를 1 올리고 갱신 시각을 남긴다 ────────────────────────────
UPDATE member
   SET point      = point + 1,
       updated_at = now()
 WHERE id IN (SELECT id FROM member ORDER BY id LIMIT :cnt);

-- ── car : 가격을 1% 올린다 (numeric(12,2) 범위 안에서 반복 실행해도 안전) ────
UPDATE car
   SET price      = (price * 1.01)::numeric(12,2),
       updated_at = now()
 WHERE id IN (SELECT id FROM car ORDER BY id LIMIT :cnt);

-- ── computer : source 에 updated_at 이 없다. 값만 흔든다 ───────────────────
UPDATE computer
   SET price_usd = (price_usd * 1.01)::numeric(10,2)
 WHERE id IN (SELECT id FROM computer ORDER BY id LIMIT :cnt);

-- ============================================================================
-- 대량 DELETE — CDC 반영 시간 측정용 (seed-bulk.sql · update-bulk.sql 의 짝)
--
--   podman exec -i emb-cdc-source-pg psql -U postgres -d sourcedb \
--       -v ON_ERROR_STOP=1 -v cnt=1000 -f - < scripts/delete-bulk.sql
--
-- 세 테이블(member/car/computer)에서 id 가 가장 작은 행부터 cnt 개씩 지운다.
-- 한 번 실행에 3 x cnt 개의 DELETE 이벤트가 흐른다.
--
-- 주의 1) UPDATE 측정과 달리 **매번 다른 행**이 대상이 된다. 지운 행은 사라지므로
--         다음 실행은 그다음으로 작은 id 를 집는다. 사다리를 순차로 올릴 때
--         (100 → 1,000 → 10,000 …) 대상이 겹치지 않는다.
-- 주의 2) 대상 선정에 ORDER BY random() 을 쓰지 않는다. 수백만 행에서 매번
--         풀스캔+정렬이 되어 재려는 CDC 가 아니라 부하 생성 쪽에서 시간을 먹는다.
-- 주의 3) member 를 먼저 지운다. member.grade_id 가 grade 를 참조하지만 grade 는
--         건드리지 않으므로 FK 위반은 없다. 순서를 고정해 두는 것은 세 스택의
--         이벤트 순서를 같게 만들기 위해서다.
-- 주의 4) 타깃에서 car 는 물리 삭제, computer/member 는 소프트 삭제로 반영된다.
--         정합성은 "타깃의 살아있는 행 수 = 소스 행 수"로 확인한다.
-- ============================================================================

-- 호출 쪽에서 cnt 를 주지 않았을 때만 기본값을 채운다.
\if :{?cnt}
\else
\set cnt 1000
\endif

DELETE FROM member
 WHERE id IN (SELECT id FROM member ORDER BY id LIMIT :cnt);

DELETE FROM car
 WHERE id IN (SELECT id FROM car ORDER BY id LIMIT :cnt);

DELETE FROM computer
 WHERE id IN (SELECT id FROM computer ORDER BY id LIMIT :cnt);

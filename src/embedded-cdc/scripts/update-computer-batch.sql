-- ============================================================================
-- computer 전량 UPDATE — 10000 행씩 끊어서 (update-bulk.sql 의 배치 판)
--
--   podman exec -i emb-cdc-source-pg psql -U postgres -d sourcedb \
--       -v ON_ERROR_STOP=1 -f - < scripts/update-computer-batch.sql
--
--   -- 배치 크기/상한/배치 간 쉬는 시간을 바꾸려면
--   ... -v batch=10000 -v maxrows=0 -v pause=0 -f - < scripts/update-computer-batch.sql
--
--     batch   한 트랜잭션에서 갱신할 행 수 (기본 10000)
--     maxrows 전체 상한. 0 이면 테이블 끝까지 (기본 0)
--     pause   배치와 배치 사이 쉬는 초. 소수 가능 (기본 0)
--
-- update-bulk.sql 은 한 문장 = 한 트랜잭션이라 cnt 를 키우면 WAL 한 덩어리가 그대로
-- 커진다. 여기서는 batch 행마다 COMMIT 해서 트랜잭션을 쪼갠다.
--
-- 주의 1) 대상은 PK 순서로 훑는다(키셋). OFFSET 도 ORDER BY random() 도 쓰지 않는다.
--         둘 다 배치가 뒤로 갈수록 느려져, 재려는 CDC 가 아니라 부하 쪽이 병목이 된다.
-- 주의 2) 배치마다 COMMIT 하므로 중간에 끊겨도 앞 배치는 남는다. 되돌리지 않는다.
-- 주의 3) computer 는 REPLICA IDENTITY FULL 이다. UPDATE 는 before 이미지까지 WAL 에
--         실리므로 같은 건수의 INSERT 보다 WAL 이 크고 반영도 느리다.
-- 주의 4) 값은 마지막 자리 1전을 올렸다 내렸다 한다. 몇 번을 돌려도 price_usd 가
--         커지지 않아 numeric(10,2) 를 넘길 걱정 없이 반복 측정할 수 있다.
-- ============================================================================

\if :{?batch}
\else
\set batch 10000
\endif

\if :{?maxrows}
\else
\set maxrows 0
\endif

\if :{?pause}
\else
\set pause 0
\endif

-- pg_temp 에 만든다. 세션이 끝나면 같이 사라져 스키마에 남지 않는다.
-- DO 블록 안에서는 COMMIT 을 못 하므로 프로시저로 둔다.
CREATE PROCEDURE pg_temp.update_computer_batched(
    p_batch   int,
    p_maxrows bigint,
    p_pause   double precision
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_id bigint := 0;      -- 여기까지 처리했다
    v_hi      bigint;           -- 이번 배치의 마지막 id
    v_rows    bigint;
    v_total   bigint := 0;
    v_batches int    := 0;
    v_limit   int;
    v_t0      timestamptz := clock_timestamp();
BEGIN
    LOOP
        v_limit := p_batch;
        -- 상한이 걸려 있으면 마지막 배치를 남은 만큼으로 줄인다.
        IF p_maxrows > 0 THEN
            EXIT WHEN v_total >= p_maxrows;
            v_limit := LEAST(p_batch, (p_maxrows - v_total)::int);
        END IF;

        -- PK 인덱스를 v_last_id 부터 v_limit 개만 훑어 이번 배치의 끝 id 를 잡는다.
        SELECT max(id)
          INTO v_hi
          FROM (SELECT id
                  FROM computer
                 WHERE id > v_last_id
                 ORDER BY id
                 LIMIT v_limit) s;

        EXIT WHEN v_hi IS NULL;   -- 남은 행이 없다

        UPDATE computer
           SET price_usd = price_usd
                         + CASE WHEN (price_usd * 100)::bigint % 2 = 0
                                THEN 0.01 ELSE -0.01 END
         WHERE id > v_last_id
           AND id <= v_hi;

        GET DIAGNOSTICS v_rows = ROW_COUNT;

        v_last_id := v_hi;
        v_total   := v_total + v_rows;
        v_batches := v_batches + 1;

        COMMIT;   -- 배치 하나 = 트랜잭션 하나. 여기서 WAL 이 끊긴다.

        RAISE NOTICE 'batch % : % rows (id <= %), 누적 % 행, %s 경과',
                     v_batches, v_rows, v_hi, v_total,
                     round(extract(epoch FROM clock_timestamp() - v_t0)::numeric, 1);

        IF p_pause > 0 THEN
            PERFORM pg_sleep(p_pause);
        END IF;
    END LOOP;

    RAISE NOTICE '완료 — % 배치, 총 % 행, %s',
                 v_batches, v_total,
                 round(extract(epoch FROM clock_timestamp() - v_t0)::numeric, 1);
END;
$$;

CALL pg_temp.update_computer_batched(:batch, :maxrows, :pause);

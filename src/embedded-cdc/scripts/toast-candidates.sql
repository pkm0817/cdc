-- ============================================================================
-- TOAST 후보 컬럼 판별 (V5 1단계 · 정적 선별)
--
-- PostgreSQL 은 큰 값을 행 밖(TOAST)에 저장하고, UPDATE 에서 그 컬럼이 바뀌지
-- 않았으면 WAL 에 새 값을 싣지 않는다. Debezium 은 그 자리를 자리표시자
-- (__debezium_unavailable_value)로 채운다. 관심 필드가 여기 걸리면 이벤트만으로는
-- 현재 값을 알 수 없다.
--
-- "관심 필드가 이 케이스에 걸리는가"를 사후가 아니라 사전에 묻기 위한 질의다.
--
--   docker exec -i emb-cdc-source-pg psql -U postgres -d sourcedb \
--     -f - < scripts/toast-candidates.sql
--
-- ── 판정 규칙 ───────────────────────────────────────────────────────────────
--   1) c.reltoastrelid <> 0   그 테이블에 TOAST 저장소가 붙어 있다
--                             (toastable 컬럼이 하나라도 있으면 붙는다)
--   2) a.attstorage <> 'p'    PLAIN 은 절대 행 밖으로 나가지 않으므로 제외
--   3) max(pg_column_size(col)) >= 2032
--                             TOAST_TUPLE_THRESHOLD (블록 8KB 의 약 1/4).
--                             pg_column_size 는 압축 후 실제 저장 크기라
--                             "잘 압축되어 인라인에 남는 큰 값"을 후보에서 뺀다
--
-- ── 이 규칙의 한계 ──────────────────────────────────────────────────────────
-- 토스터는 컬럼이 아니라 행 전체가 임계를 넘을 때 동작하고, 그때 가장 큰
-- 속성부터 밖으로 내보낸다. 따라서 임계보다 작은 컬럼도 행이 넓으면 밖으로 나갈 수
-- 있다. 그래서 이 질의는 1단계 선별이고, 확정은 2단계에서 한다 —
-- 관심 필드를 건드리지 않는 UPDATE 를 한 건 흘려보내 이벤트에 자리표시자가 오는지
-- 본다 (V5ReplicaIdentityCostTest 가 그 절차를 그대로 수행한다).
--
-- 전 행 스캔이라 큰 테이블에서는 비싸다. 상시 모니터링이 아니라 스키마가 바뀔 때
-- 돌리는 점검용이다.
-- ============================================================================
WITH toastable AS (
    SELECT c.oid,
           n.nspname   AS schema_name,
           c.relname   AS table_name,
           a.attname   AS column_name,
           a.attstorage,
           format_type(a.atttypid, a.atttypmod) AS data_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    WHERE c.relkind = 'r'
      AND n.nspname = 'public'
      AND c.reltoastrelid <> 0
      AND a.attstorage <> 'p'
)
SELECT t.schema_name,
       t.table_name,
       t.column_name,
       t.data_type,
       CASE t.attstorage
           WHEN 'x' THEN 'EXTENDED'
           WHEN 'e' THEN 'EXTERNAL'
           WHEN 'm' THEN 'MAIN'
           ELSE t.attstorage::text
       END                                AS storage,
       COALESCE(m.max_size, 0)            AS max_stored_bytes,
       COALESCE(m.max_size, 0) >= 2032    AS toast_candidate
FROM toastable t
-- SQL 은 식별자를 변수로 받지 못한다. 함수를 새로 만들지 않고 동적 SQL 을 쓰는
-- 유일한 내장 수단이 query_to_xml 이다 (exporter 의 행 수 질의와 같은 방식).
CROSS JOIN LATERAL (
    SELECT (xpath('/row/c/text()',
                  query_to_xml(format('SELECT max(pg_column_size(%I))::bigint AS c FROM %I.%I',
                                      t.column_name, t.schema_name, t.table_name),
                               false, true, '')))[1]::text::bigint AS max_size
) m
ORDER BY toast_candidate DESC, max_stored_bytes DESC, t.table_name, t.column_name;

-- ============================================================================
-- 대사 질의 (V6) — 건수 + 체크섬, 전체와 최근 구간을 함께 낸다
--
-- 양쪽 DB 에서 같은 질의를 돌리고 결과를 비교한다.
--
--   docker exec -i emb-cdc-source-pg psql -U postgres -d sourcedb -f - < scripts/reconcile.sql
--   docker exec -i emb-cdc-target-pg psql -U postgres -d targetdb -f - < scripts/reconcile.sql
--
-- ── 왜 구간을 따로 내는가 ────────────────────────────────────────────────────
-- 체크섬은 md5(string_agg(...)) 라 정렬을 동반한 전체 스캔이다. 표가 커지면
-- 상시로 돌릴 수 없다. 그래서 두 가지를 같이 낸다.
--
--   full_*    : 전체 대사. 야간 배치로 하루 한 번.
--   window_*  : 최근 구간(기본 24시간) 대사. 상시(수십 분 주기)로 돌린다.
--
-- 구간 대사는 그 구간 밖에서 조용히 어긋난 행을 못 잡는다. 두 개는 대체재가 아니라
-- 짝이다 — 상시로 빨리 잡고, 야간에 전부 훑는다.
--
-- 구간 컬럼은 이름 규칙으로 찾는다: updated_at > created_at > synced_at 순.
-- 원천 시각을 먼저 고르는 것이 중요하다. synced_at 은 수신이 적재한 시각이라
-- 원천의 "최근 24시간"과 다른 집합을 가리킨다 — 3일 전에 만들어진 행이 오늘 재적재되면
-- 수신 구간에는 들어오고 원천 구간에는 없다. 양쪽이 같은 의미의 컬럼을 볼 때만
-- 구간 대사가 성립하므로, 수신 표가 원천 시각을 보존하지 않으면 그 표는 구간 대사를 할 수 없다.
--
-- 구간 대사가 실제로 빨라지려면 그 컬럼에 인덱스가 있어야 한다 — 없으면 조건만
-- 붙은 전체 스캔이라 전체 대사와 비용이 같다.
--
-- ── 체크섬이 비교 가능한 조건 ────────────────────────────────────────────────
-- 이 파이프라인은 표에 따라 스키마를 변환한다(computer). 변환된 표는 양쪽 컬럼이
-- 달라 체크섬을 견줄 수 없고 건수 대사만 성립한다.
--
-- 수신 측에만 있는 관리 컬럼(deleted, source_lsn, synced_at)은 체크섬에서 뺀다.
-- 그래서 비교 기준은 column_list 가 아니라 checksum_columns 다 — 그 셋을 뺀 뒤의
-- 컬럼 집합이 양쪽에서 같으면 체크섬을 견줄 수 있다.
-- 소프트 삭제를 쓰는 표는 살아 있는 행만 센다 — 원천에는 지워진 행이 없기 때문이다.
--
-- 전제: 양쪽 세션의 TimeZone 이 같아야 한다. timestamptz 의 텍스트 표현이 세션
-- 시간대를 따르므로, 다르면 값이 같아도 체크섬이 갈린다. (두 컨테이너 모두 UTC)
-- ============================================================================
WITH shape AS (
    SELECT t.schemaname AS schema_name,
           t.tablename  AS table_name,
           string_agg(c.column_name, ',' ORDER BY c.column_name) AS column_list,
           -- 체크섬 비교의 기준. 관리 컬럼을 뺀 뒤의 집합이 같아야 견줄 수 있다.
           string_agg(c.column_name, ',' ORDER BY c.column_name)
               FILTER (WHERE c.column_name NOT IN ('deleted', 'source_lsn', 'synced_at'))
                                                                 AS checksum_columns,
           bool_or(c.column_name = 'deleted')                    AS soft_delete,
           COALESCE(
               max(c.column_name) FILTER (WHERE c.column_name = 'updated_at'),
               max(c.column_name) FILTER (WHERE c.column_name = 'created_at'),
               max(c.column_name) FILTER (WHERE c.column_name = 'synced_at')
           ) AS window_column
    FROM pg_tables t
    JOIN information_schema.columns c
      ON c.table_schema = t.schemaname AND c.table_name = t.tablename
    WHERE t.schemaname = 'public'
      -- 파이프라인 자신의 살림살이와 검증 픽스처는 대사 대상이 아니다
      AND t.tablename NOT LIKE 'cdc\_%'
      AND t.tablename NOT LIKE 'verify\_%'
    GROUP BY t.schemaname, t.tablename
)
SELECT sh.table_name,
       sh.checksum_columns,
       sh.window_column,
       COALESCE((xpath('/row/c/text()', f.x))[1]::text::bigint, 0) AS full_rows,
       COALESCE((xpath('/row/k/text()', f.x))[1]::text, '')        AS full_checksum,
       (xpath('/row/c/text()', w.x))[1]::text::bigint              AS window_rows,
       (xpath('/row/k/text()', w.x))[1]::text                      AS window_checksum
FROM shape sh
-- SQL 은 식별자를 변수로 받지 못한다. 함수를 새로 만들지 않고 동적 SQL 을 쓰는 유일한
-- 내장 수단이 query_to_xml 이다 (exporter 의 행 수 질의와 같은 방식).
CROSS JOIN LATERAL (
    SELECT query_to_xml(
        format('SELECT count(*) AS c, '
               'coalesce(md5(string_agg((to_jsonb(t) - ''deleted'' - ''source_lsn'' - ''synced_at'')::text, '
               '                        '','' ORDER BY t.id)), '''') AS k '
               'FROM %I.%I t%s',
               sh.schema_name, sh.table_name,
               CASE WHEN sh.soft_delete THEN ' WHERE t.deleted = false' ELSE '' END),
        false, true, '') AS x
) f
CROSS JOIN LATERAL (
    -- 구간 컬럼이 없으면 질의 자체를 만들지 않는다 (CASE 가 짧게 끊는다)
    SELECT CASE WHEN sh.window_column IS NULL THEN NULL ELSE query_to_xml(
        format('SELECT count(*) AS c, '
               'coalesce(md5(string_agg((to_jsonb(t) - ''deleted'' - ''source_lsn'' - ''synced_at'')::text, '
               '                        '','' ORDER BY t.id)), '''') AS k '
               'FROM %I.%I t WHERE %s t.%I >= now() - interval ''24 hours''',
               sh.schema_name, sh.table_name,
               CASE WHEN sh.soft_delete THEN 't.deleted = false AND' ELSE '' END,
               sh.window_column),
        false, true, '') END AS x
) w
ORDER BY sh.table_name;

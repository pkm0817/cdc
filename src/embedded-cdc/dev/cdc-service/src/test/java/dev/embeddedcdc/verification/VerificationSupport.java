package dev.embeddedcdc.verification;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.FieldDiff;

import java.math.BigDecimal;
import java.time.Duration;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

/**
 * 검증 테스트 공통 준비물.
 *
 * 캡처 대상은 운영 테이블(car, computer)이 아니라 검증 전용 테이블을 쓴다.
 * 기동 중인 emb-cdc-service 의 publication·슬롯과 겹치면 서로의 결과를 오염시킨다.
 */
public abstract class VerificationSupport {

    protected static final String RECORD_TABLE = "verify_record";
    protected static final String RECORD_PUBLICATION = "verify_record_pub";

    /**
     * 실적 레코드를 본뜬 검증 테이블.
     * payload 는 TOAST 임계(약 2KB)를 넘길 수 있는 대용량 필드로, V5 에서 쓴다.
     */
    protected static void createRecordFixture() {
        Db.onSource("""
                CREATE TABLE IF NOT EXISTS verify_record (
                    id          BIGSERIAL     PRIMARY KEY,
                    biz_key     TEXT          NOT NULL,
                    amount      NUMERIC(14,2) NOT NULL,
                    status      TEXT          NOT NULL,
                    payload     TEXT,
                    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
                )
                """);
        // UPDATE/DELETE 이벤트의 before 이미지에 전체 컬럼을 담기 위한 설정.
        // 기본값(DEFAULT)이면 before 에 PK 만 실려 필드 단위 변경 식별이 불가능하다.
        Db.onSource("ALTER TABLE verify_record REPLICA IDENTITY FULL");
        recreatePublication(RECORD_PUBLICATION, RECORD_TABLE);
    }

    protected static void recreatePublication(String publication, String table) {
        Db.onSource(
                "DROP PUBLICATION IF EXISTS " + publication,
                "CREATE PUBLICATION " + publication + " FOR TABLE " + table);
    }

    protected static void truncateRecords() {
        // TRUNCATE 는 op=t 라 캡처되지 않는다. 검증 데이터를 지울 때는 오히려 그 편이 낫다 —
        // 정리 작업이 이벤트로 흘러 들어와 다음 테스트를 오염시키지 않는다.
        Db.onSource("TRUNCATE TABLE verify_record RESTART IDENTITY");
    }

    // ── 복제 슬롯 관측 ─────────────────────────────────────────────────────

    protected static boolean slotExists(String slot) {
        Long n = Db.scalarOnSource(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ?", Long.class, slot);
        return n != null && n > 0;
    }

    protected static void dropSlotQuietly(String slot) {
        if (!slotExists(slot)) {
            return;
        }
        try {
            Db.onSource("SELECT pg_drop_replication_slot('" + slot + "')");
        } catch (RuntimeException e) {
            // active 인 슬롯은 삭제되지 않는다. 엔진이 아직 붙어 있다는 뜻이므로 잠깐 뒤 재시도한다.
            sleep(1000);
            if (slotExists(slot)) {
                Db.onSource("SELECT pg_drop_replication_slot('" + slot + "')");
            }
        }
    }

    /** 슬롯이 붙잡고 있어 아직 지우지 못하는 WAL 크기(바이트). 다운타임 허용치 산출의 근거다. */
    protected static long slotRetainedWalBytes(String slot) {
        Long bytes = Db.scalarOnSource("""
                SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
                FROM pg_replication_slots WHERE slot_name = ?
                """, Long.class, slot);
        return bytes == null ? -1 : bytes;
    }

    protected static String slotConfirmedFlushLsn(String slot) {
        return Db.scalarOnSource(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = ?",
                String.class, slot);
    }

    protected static boolean slotActive(String slot) {
        Boolean active = Db.scalarOnSource(
                "SELECT active FROM pg_replication_slots WHERE slot_name = ?", Boolean.class, slot);
        return Boolean.TRUE.equals(active);
    }

    // ── 데이터 투입 ────────────────────────────────────────────────────────

    protected static void insertRecord(String bizKey, BigDecimal amount, String status) {
        Db.onSource("INSERT INTO verify_record (biz_key, amount, status) VALUES ("
                + "'" + bizKey + "', " + amount + ", '" + status + "')");
    }

    /** 한 트랜잭션으로 n 건을 넣는다. 대량 변경 처리량 측정용. */
    protected static void insertRecordsInOneTransaction(String prefix, int n) {
        Db.inSourceTransaction(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO verify_record (biz_key, amount, status) VALUES (?, ?, ?)")) {
                for (int i = 1; i <= n; i++) {
                    ps.setString(1, prefix + "-" + i);
                    ps.setBigDecimal(2, BigDecimal.valueOf(1000L + i));
                    ps.setString(3, "NEW");
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new IllegalStateException("대량 INSERT 실패", e);
            }
        });
    }

    // ── 이벤트 판독 ────────────────────────────────────────────────────────

    /**
     * before 와 after 를 비교해 "실제로 바뀐 필드"만 뽑는다.
     * V1 의 필드 단위 변경 식별이 가능한지가 이 메서드가 값을 돌려주느냐로 판정된다.
     *
     * 판정식은 운영 코드의 FieldDiff 를 그대로 쓴다. 검증용 비교를 따로 두면
     * 검증에서 통과한 것과 운영이 감사 로그에 남기는 것이 서로 다른 것이 된다.
     */
    protected static Set<String> changedFields(ChangeEvent event) {
        return FieldDiff.between(event.before(), event.after()).changed();
    }

    /** Debezium 이 값을 실을 수 없을 때 채워 넣는 자리표시자. V5 의 판정 기준이다. */
    protected static final String TOAST_PLACEHOLDER = FieldDiff.UNAVAILABLE;

    // ── 지연 측정과 시계 편차 ──────────────────────────────────────────────

    /**
     * 목표 지연. V1 통과 기준이며, 운영 경보 CdcLatencyBudgetExceeded 의 임계와 같은 값이다.
     * 한쪽만 바꾸면 검증에서 통과한 것이 운영에서는 경보가 되거나 그 반대가 된다.
     */
    protected static final Duration LATENCY_BUDGET = Duration.ofSeconds(5);

    /**
     * 시계 편차 허용치 = 목표 지연의 10%.
     *
     * 커밋 기준 지연은 source 의 ts_ms(DB 시계)와 이 프로세스 시계의 차이다.
     * 두 시계가 어긋나면 그 차이가 지연 수치에 통째로 섞인다 — 1차 실행에서
     * 편차 3,561ms 에 지연 526ms 가 나와 재려던 값보다 오차가 7배 컸다.
     * 오차가 재려는 값의 10% 를 넘으면 그 회차의 커밋 기준 지연은 판정에 쓰지 않는다.
     */
    protected static final Duration SKEW_TOLERANCE =
            Duration.ofMillis(LATENCY_BUDGET.toMillis() / 10);

    /**
     * source DB 시계 - 이 프로세스 시계 (밀리초).
     *
     * 질의 직전·직후 로컬 시각의 중간값과 견준다. 보정하지 않으면 왕복 시간의 절반이
     * 편차로 계상되어, 시계가 맞는 환경에서도 수 ms 가 계속 찍힌다.
     * 운영의 SourceClockSkewProbe 와 같은 계산식이다.
     */
    protected static long clockSkewMs() {
        long before = System.currentTimeMillis();
        Long dbNowMs = Db.scalarOnSource(
                "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint", Long.class);
        long after = System.currentTimeMillis();
        return dbNowMs == null ? 0 : dbNowMs - (before + after) / 2;
    }

    /**
     * 한 회차의 지연 계측 결과.
     *
     * @param roundTripMs 변경을 넣은 시각부터 이벤트를 받은 시각까지. 로컬 시계 하나로만
     *                    재므로 시계 편차와 무관하다 — 통과/실패 판정은 이 값으로 한다
     * @param commitMs    source 커밋 시각(ts_ms) 기준 지연. 운영의
     *                    cdc_end_to_end_lag_seconds 와 같은 계산식이라 편차가 섞인다
     * @param skewMs      측정 시점의 시계 편차
     * @param valid       편차가 허용치 안이라 commitMs 를 판정에 쓸 수 있는지
     */
    protected record LatencySample(long roundTripMs, long commitMs, long skewMs, boolean valid) {
    }

    /**
     * 왕복 지연과 커밋 기준 지연을 함께 재서 기록한다.
     *
     * 두 수치를 나눠 남기는 이유: 운영 지표는 커밋 기준(ts_ms)이라 편차에 오염되는데,
     * 검증에서 왕복 지연만 남기면 그 오염이 있었는지 없었는지를 보고서에서 알 수 없다.
     */
    protected static LatencySample recordLatency(String item, String name,
                                                 long sentAtMs, ChangeEvent event) {
        long receivedAt = System.currentTimeMillis();
        long skew = clockSkewMs();
        long roundTrip = receivedAt - sentAtMs;
        long commit = receivedAt + skew - event.sourceTsMs();
        boolean valid = Math.abs(skew) <= SKEW_TOLERANCE.toMillis();

        VerificationReport.metric(item, name + " 왕복 지연", roundTrip + " ms");
        VerificationReport.metric(item, name + " 커밋 기준 지연(편차 보정)",
                commit + " ms" + (valid ? "" : "  ← 무효"));
        VerificationReport.metric(item, name + " 시계 편차",
                skew + " ms (허용 " + SKEW_TOLERANCE.toMillis() + " ms)");

        if (!valid) {
            VerificationReport.note(item, name + ": 시계 편차 " + skew + " ms 가 허용치 "
                    + SKEW_TOLERANCE.toMillis() + " ms 를 넘었다 — 규칙에 따라 이 회차의 "
                    + "커밋 기준 지연은 판정에 쓰지 않는다. 왕복 지연으로만 판정한다");
        }
        return new LatencySample(roundTrip, commit, skew, valid);
    }

    protected static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected static Connection openSource() throws SQLException {
        return Db.source();
    }
}

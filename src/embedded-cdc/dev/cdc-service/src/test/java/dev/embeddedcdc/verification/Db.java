package dev.embeddedcdc.verification;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 검증 테스트가 쓰는 DB 접근 도구.
 *
 * 운영 코드의 DataSource 를 쓰지 않는다 — 검증은 스프링 컨텍스트 없이 돌아야
 * 엔진을 켜고 끄는 시나리오(V2, V4)를 자유롭게 만들 수 있다.
 */
public final class Db {

    private static final String SOURCE_URL =
            System.getProperty("cdc.verify.source.url", "jdbc:postgresql://localhost:56432/sourcedb");
    private static final String TARGET_URL =
            System.getProperty("cdc.verify.target.url", "jdbc:postgresql://localhost:56433/targetdb");
    private static final String USER = System.getProperty("cdc.verify.user", "postgres");
    private static final String PASSWORD = System.getProperty("cdc.verify.password", "postgres");

    private Db() {
    }

    public static Connection source() throws SQLException {
        return DriverManager.getConnection(SOURCE_URL, USER, PASSWORD);
    }

    public static Connection target() throws SQLException {
        return DriverManager.getConnection(TARGET_URL, USER, PASSWORD);
    }

    // ── Debezium 엔진 설정이 필요로 하는 접속 정보 조각 ──────────────────────

    public static String host() {
        return uri().getHost();
    }

    public static int port() {
        return uri().getPort();
    }

    public static String database() {
        String path = uri().getPath();
        return path.startsWith("/") ? path.substring(1) : path;
    }

    public static String user() {
        return USER;
    }

    public static String password() {
        return PASSWORD;
    }

    private static URI uri() {
        // jdbc:postgresql://host:port/db 에서 jdbc: 를 떼면 표준 URI 로 파싱된다
        return URI.create(SOURCE_URL.substring("jdbc:".length()));
    }

    // ── 실행 도우미 ────────────────────────────────────────────────────────

    public static void onSource(String... sql) {
        exec(Db::source, sql);
    }

    public static void onTarget(String... sql) {
        exec(Db::target, sql);
    }

    private static void exec(SqlSupplier<Connection> supplier, String... statements) {
        try (Connection c = supplier.get(); Statement s = c.createStatement()) {
            for (String sql : statements) {
                s.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQL 실행 실패: " + String.join(" | ", statements), e);
        }
    }

    /** 갱신 문장을 실행하고 실제로 영향받은 행 수를 돌려준다. 0 이면 조건절에 걸려 차단된 것이다. */
    public static int updateOnTarget(String sql) {
        try (Connection c = target(); Statement s = c.createStatement()) {
            return s.executeUpdate(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("갱신 실패: " + sql, e);
        }
    }

    /** 스칼라 하나를 읽는다. 없으면 null. */
    public static <T> T scalarOnSource(String sql, Class<T> type, Object... args) {
        return scalar(Db::source, sql, type, args);
    }

    public static <T> T scalarOnTarget(String sql, Class<T> type, Object... args) {
        return scalar(Db::target, sql, type, args);
    }

    private static <T> T scalar(SqlSupplier<Connection> supplier, String sql, Class<T> type, Object... args) {
        try (Connection c = supplier.get(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, args);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getObject(1, type) : null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("조회 실패: " + sql, e);
        }
    }

    public static List<Object[]> rowsOnSource(String sql, Object... args) {
        return rows(Db::source, sql, args);
    }

    public static List<Object[]> rowsOnTarget(String sql, Object... args) {
        return rows(Db::target, sql, args);
    }

    private static List<Object[]> rows(SqlSupplier<Connection> supplier, String sql, Object... args) {
        try (Connection c = supplier.get(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, args);
            try (ResultSet rs = ps.executeQuery()) {
                int cols = rs.getMetaData().getColumnCount();
                List<Object[]> rows = new ArrayList<>();
                while (rs.next()) {
                    Object[] row = new Object[cols];
                    for (int i = 0; i < cols; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    rows.add(row);
                }
                return rows;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("조회 실패: " + sql, e);
        }
    }

    /** 한 트랜잭션 안에서 여러 문장을 실행한다. 대량 변경 시나리오용. */
    public static void inSourceTransaction(Consumer<Connection> work) {
        try (Connection c = source()) {
            c.setAutoCommit(false);
            work.accept(c);
            c.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("트랜잭션 실패", e);
        }
    }

    private static void bind(PreparedStatement ps, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
    }

    @FunctionalInterface
    private interface SqlSupplier<T> {
        T get() throws SQLException;
    }
}

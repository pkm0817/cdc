package dev.embeddedcdc.infrastructure.cdc;

import dev.embeddedcdc.domain.model.FailureVerdict;
import dev.embeddedcdc.domain.port.out.FailureClassifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * SQLState 로 실패의 성격을 가른다.
 *
 * 스프링의 예외 계층(TransientDataAccessException 등)을 쓰지 않는 이유가 있다 —
 * 커넥션 실패를 뜻하는 DataAccessResourceFailureException 이 NonTransient 밑에 있어서,
 * instanceof 로 가르면 <b>커넥션 장애가 격리 대상이 된다</b>. SQLState 가 훨씬 정확하다.
 *
 * 판정하지 못한 예외는 RETRY 로 본다. 한 번 더 시도해 보고 그래도 안 되면
 * 건 단위 격리에서 어차피 DLQ 로 간다 — 모르는 것을 성급히 버리지 않기 위한 기본값이다.
 */
@Component
@Slf4j
public class SqlStateFailureClassifier implements FailureClassifier {

    /** 시간이 지나면 저절로 풀린다. */
    private static final Set<String> RETRY_CLASSES = Set.of("08", "53", "57");
    private static final Set<String> RETRY_CODES = Set.of("40001", "40P01", "55P03");

    /** 그 이벤트 하나의 데이터 문제다. */
    private static final Set<String> DEAD_LETTER_CLASSES = Set.of("22", "23");

    /** 구조 문제다. 모든 이벤트가 같은 이유로 실패한다. */
    private static final Set<String> HALT_CLASSES = Set.of("28", "3D", "3F", "42");

    @Override
    public FailureVerdict classify(Throwable cause) {
        SQLException sql = findSqlException(cause);
        if (sql != null) {
            FailureVerdict bySqlState = bySqlState(sql.getSQLState());
            if (bySqlState != null) {
                return bySqlState;
            }
        }
        if (isDataProblem(cause)) {
            return FailureVerdict.DEAD_LETTER;
        }
        log.debug("분류하지 못한 예외 — 우선 재시도로 본다: {}", cause.toString());
        return FailureVerdict.RETRY;
    }

    private FailureVerdict bySqlState(String sqlState) {
        if (sqlState == null || sqlState.length() < 2) {
            return null;
        }
        if (RETRY_CODES.contains(sqlState)) {
            return FailureVerdict.RETRY;
        }
        String category = sqlState.substring(0, 2);
        if (RETRY_CLASSES.contains(category)) {
            return FailureVerdict.RETRY;
        }
        if (DEAD_LETTER_CLASSES.contains(category)) {
            return FailureVerdict.DEAD_LETTER;
        }
        if (HALT_CLASSES.contains(category)) {
            return FailureVerdict.HALT;
        }
        return null;
    }

    /**
     * 이벤트 값 자체를 읽지 못한 경우. 같은 값을 몇 번 넣어도 결과가 같으므로 격리 대상이다.
     * RowData 의 판독 메서드와 매퍼에서 나온다.
     */
    private boolean isDataProblem(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof NumberFormatException
                    || t instanceof DateTimeParseException
                    || t instanceof ArithmeticException
                    || t instanceof IllegalStateException
                    || t instanceof IllegalArgumentException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    private SQLException findSqlException(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql) {
                return sql;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }
}

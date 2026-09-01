package dev.embeddedcdc.domain.model;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 이벤트에 실려 온 행 하나. 컬럼명에서 값(문자열)으로 가는 map 이다.
 *
 * 도메인이 Jackson 을 모르게 하려고 두는 경계다. 값을 전부 문자열로 받아도 손실이 없다 —
 * decimal.handling.mode=string 이라 NUMERIC 이 이미 문자열로 도착하고,
 * 나머지 타입도 문자열에서 원래 타입으로 되돌릴 수 있다.
 *
 * 컬럼이 없거나 null 이면 조용히 기본값을 주지 않고 예외를 던진다.
 * 조용한 기본값은 잘못된 값을 target 에 적재하고, 그 사실이 한참 뒤에 발견된다.
 */
public record RowData(Map<String, String> values) {

    public RowData {
        // Map.copyOf 는 null 값을 거부하므로 쓸 수 없다. 컬럼이 null 인 경우도 담아야 한다.
        values = Collections.unmodifiableMap(new HashMap<>(values));
    }

    public String text(String column) {
        return require(column);
    }

    public long longValue(String column) {
        return Long.parseLong(require(column));
    }

    public int intValue(String column) {
        return Integer.parseInt(require(column));
    }

    public BigDecimal decimal(String column) {
        return new BigDecimal(require(column));
    }

    /** source 의 timestamptz. Debezium 이 UTC 기준 ISO-8601 문자열로 보낸다. */
    public OffsetDateTime timestamp(String column) {
        return OffsetDateTime.parse(require(column));
    }

    private String require(String column) {
        String value = values.get(column);
        if (value == null) {
            throw new IllegalStateException(
                    "이벤트에 컬럼이 없거나 null 이다: " + column + " (수신한 컬럼: " + values.keySet() + ")");
        }
        return value;
    }
}

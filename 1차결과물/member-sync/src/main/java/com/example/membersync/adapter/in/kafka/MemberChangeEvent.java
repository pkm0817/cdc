package com.example.membersync.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Debezium 변경 이벤트. 커넥터를 {@code value.converter.schemas.enable=false} 로 띄웠으므로
 * 메시지 최상위가 곧 payload 다 (schema 블록 없음).
 *
 * <pre>
 * { "before": {...}, "after": {...}, "source": { "lsn": 24023128, "ts_ms": ... },
 *   "op": "u", "ts_ms": ... }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MemberChangeEvent(
        MemberRow before,
        MemberRow after,
        Source source,
        String op,
        @JsonProperty("ts_ms") long tsMs
) {

    /** PostgreSQL {@code members} 테이블의 행. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MemberRow(
            @JsonProperty("member_id")     Long   memberId,
            @JsonProperty("full_name")     String fullName,
            @JsonProperty("email_address") String emailAddress,
            String status
    ) {}

    /**
     * {@code lsn} 은 PostgreSQL 에서만 채워지는 단조 증가 정수라 적재 순서 판정 키로 쓴다.
     * MySQL 은 {@code file}+{@code pos} 복합값이라 이 용법이 안 된다.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Source(
            long lsn,
            @JsonProperty("ts_ms") long tsMs
    ) {}
}

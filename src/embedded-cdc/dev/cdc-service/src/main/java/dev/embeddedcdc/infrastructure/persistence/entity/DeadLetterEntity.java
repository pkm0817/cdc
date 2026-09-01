package dev.embeddedcdc.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

/**
 * 반영하지 못한 이벤트 한 건.
 *
 * 여기 있는 값만으로 원래 이벤트를 재구성할 수 있어야 한다 —
 * 그러지 못하면 "격리"가 아니라 "유실 기록"일 뿐이다.
 *
 * 여기 id 는 target 이 발번한다. car / computer 와 달리 source 에서 온 값이 아니므로
 * GenerationType.IDENTITY 를 쓴다.
 */
@Entity
@Table(name = "cdc_dead_letter")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DeadLetterEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String pipeline;

    @Column(name = "source_table", nullable = false)
    private String sourceTable;

    @Column(nullable = false)
    private String op;

    @Column(name = "source_lsn", nullable = false)
    private Long sourceLsn;

    /** 원본 이벤트를 재구성하기 위한 값 전체. */
    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "failure_type", nullable = false)
    private String failureType;

    @Column(name = "failure_sql_state")
    private String failureSqlState;

    @Column(name = "failure_message", nullable = false, columnDefinition = "text")
    private String failureMessage;

    @Column(nullable = false)
    private Integer attempts;

    /** PENDING · RETRY_REQUESTED · RESOLVED · DISCARDED */
    @Column(nullable = false)
    private String status;

    @Column(name = "first_failed_at", nullable = false)
    private OffsetDateTime firstFailedAt;

    @Column(name = "last_failed_at", nullable = false)
    private OffsetDateTime lastFailedAt;

    public DeadLetterEntity(String pipeline, String sourceTable, String op, Long sourceLsn,
                            String payload, String failureType, String failureSqlState,
                            String failureMessage, Integer attempts, OffsetDateTime failedAt) {
        this.pipeline = pipeline;
        this.sourceTable = sourceTable;
        this.op = op;
        this.sourceLsn = sourceLsn;
        this.payload = payload;
        this.failureType = failureType;
        this.failureSqlState = failureSqlState;
        this.failureMessage = failureMessage;
        this.attempts = attempts;
        this.status = "PENDING";
        this.firstFailedAt = failedAt;
        this.lastFailedAt = failedAt;
    }
}

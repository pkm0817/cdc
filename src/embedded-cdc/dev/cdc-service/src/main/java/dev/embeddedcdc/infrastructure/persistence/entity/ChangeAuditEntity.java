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
 * UPDATE 한 건의 필드 단위 변경 이력.
 *
 * before/after 를 통째로 남기지 않고 <b>바뀐 필드만</b> 담는다. 통째로 남기면
 * 이 표가 원본 테이블보다 빨리 커지고, 정작 읽을 때는 매번 diff 를 다시 떠야 한다.
 *
 * identifiable=false 인 행은 "바뀐 게 없었다"가 아니라 "판정을 못 했다"는 뜻이다 —
 * 그 테이블의 REPLICA IDENTITY 가 FULL 이 아니라는 신호다.
 */
@Entity
@Table(name = "cdc_change_audit")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ChangeAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String pipeline;

    @Column(name = "source_table", nullable = false)
    private String sourceTable;

    /** 변경된 행의 PK 값. 어느 행 이야기인지 못 짚으면 이력으로서 쓸모가 없다. */
    @Column(name = "row_key", nullable = false)
    private String rowKey;

    @Column(name = "source_lsn", nullable = false)
    private Long sourceLsn;

    /** source 커밋 시각. 감사 이력은 적재 시각이 아니라 원천 시각으로 읽어야 한다. */
    @Column(name = "source_ts", nullable = false)
    private OffsetDateTime sourceTs;

    /** before/after 가 둘 다 있어 필드 단위 판정이 성립했는지. */
    @Column(nullable = false)
    private Boolean identifiable;

    /** 쉼표로 이은 컬럼명. 절대 지표 레이블로 쓰지 않는다 (카디널리티). */
    @Column(name = "changed_fields", nullable = false, columnDefinition = "text")
    private String changedFields;

    /** TOAST 자리표시자가 와서 판정에서 제외한 컬럼. 비어 있는 것이 정상이다. */
    @Column(name = "unreadable_fields", columnDefinition = "text")
    private String unreadableFields;

    /** 바뀐 필드에 한정한 변경 전/후 값. JSON 문자열. */
    @Column(name = "before_values", columnDefinition = "text")
    private String beforeValues;

    @Column(name = "after_values", columnDefinition = "text")
    private String afterValues;

    @Column(name = "recorded_at", nullable = false)
    private OffsetDateTime recordedAt;

    public ChangeAuditEntity(String pipeline, String sourceTable, String rowKey, Long sourceLsn,
                             OffsetDateTime sourceTs, Boolean identifiable, String changedFields,
                             String unreadableFields, String beforeValues, String afterValues) {
        this.pipeline = pipeline;
        this.sourceTable = sourceTable;
        this.rowKey = rowKey;
        this.sourceLsn = sourceLsn;
        this.sourceTs = sourceTs;
        this.identifiable = identifiable;
        this.changedFields = changedFields;
        this.unreadableFields = unreadableFields;
        this.beforeValues = beforeValues;
        this.afterValues = afterValues;
        this.recordedAt = OffsetDateTime.now();
    }
}

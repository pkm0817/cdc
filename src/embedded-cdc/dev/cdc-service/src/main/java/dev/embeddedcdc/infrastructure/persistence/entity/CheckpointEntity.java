package dev.embeddedcdc.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

/**
 * 파이프라인 진행 지점. 파이프라인 이름 하나당 한 행이다.
 *
 * 수신 측 DB 에 두는 것이 핵심이다 — Provider 볼륨이 통째로 사라져도 살아남아,
 * 재기동 때 "우리가 어디까지 봤는지"를 말해 준다.
 */
@Entity
@Table(name = "cdc_checkpoint")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CheckpointEntity {

    @Id
    @Column(name = "pipeline")
    private String pipeline;

    @Column(name = "last_applied_lsn", nullable = false)
    private Long lastAppliedLsn;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public CheckpointEntity(String pipeline, Long lastAppliedLsn, OffsetDateTime updatedAt) {
        this.pipeline = pipeline;
        this.lastAppliedLsn = lastAppliedLsn;
        this.updatedAt = updatedAt;
    }
}

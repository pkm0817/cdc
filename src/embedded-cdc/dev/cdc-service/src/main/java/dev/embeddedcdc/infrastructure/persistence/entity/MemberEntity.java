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
 * target 의 member 테이블 매핑.
 *
 * <b>@ManyToOne 이 없다.</b> source 에는 FK 가 있지만 target 에는 없고, 엔티티에도 두지 않는다.
 * 연관을 매핑하면 JPA 가 grade 행의 존재를 전제하게 되는데, CDC 적재 시점에는
 * 그 전제가 성립하지 않을 수 있다. grade_id 는 값으로만 들고 있는다 —
 * 조인이 필요하면 조회하는 쪽에서 하면 된다.
 */
@Entity
@Table(name = "member")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED) // JPA 전용
public class MemberEntity {

    @Id
    private Long id;

    @Column(nullable = false)
    private String email;

    @Column(nullable = false)
    private String name;

    @Column(name = "grade_id", nullable = false)
    private Long gradeId;

    @Column(nullable = false)
    private Integer point;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(nullable = false)
    private boolean deleted;

    @Column(name = "source_lsn", nullable = false)
    private Long sourceLsn;

    @Column(name = "synced_at", nullable = false)
    private OffsetDateTime syncedAt;
}

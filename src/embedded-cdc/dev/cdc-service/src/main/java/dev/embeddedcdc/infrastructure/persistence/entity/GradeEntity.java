package dev.embeddedcdc.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * target 의 grade 테이블 매핑.
 *
 * ComputerEntity 와 같이 읽기 전용이다 — 쓰기는 전부 네이티브 UPSERT 와 QueryDSL UPDATE 가 한다.
 * 엔티티를 두는 이유도 같다: ddl-auto=validate 의 스키마 드리프트 탐지와 JPQL 표현.
 */
@Entity
@Table(name = "grade")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED) // JPA 전용
public class GradeEntity {

    @Id
    private Long id;

    @Column(nullable = false)
    private String code;

    @Column(nullable = false)
    private String name;

    @Column(name = "discount_rate", nullable = false, precision = 5, scale = 2)
    private BigDecimal discountRate;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(nullable = false)
    private boolean deleted;

    @Column(name = "source_lsn", nullable = false)
    private Long sourceLsn;

    @Column(name = "synced_at", nullable = false)
    private OffsetDateTime syncedAt;
}

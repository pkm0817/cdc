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
 * target 의 computer 테이블 매핑.
 *
 * 이 엔티티로는 쓰지 않는다 — 쓰기는 전부 ComputerJpaRepository 의 UPSERT/UPDATE 문이 담당한다.
 * 그럼에도 엔티티를 두는 이유는 두 가지다:
 *   1) ddl-auto=validate 가 기동 시점에 스키마 드리프트를 잡아 준다
 *      (target 테이블이 지워지거나 컬럼이 바뀌면 기동이 실패한다)
 *   2) 소프트 삭제를 JPQL 로 표현할 수 있게 된다
 *
 * 쓰기 경로가 없으므로 생성자도 기본 생성자 하나뿐이다.
 */
@Entity
@Table(name = "computer")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED) // JPA 전용
public class ComputerEntity {

    @Id
    private Long id;

    @Column(name = "full_name", nullable = false)
    private String fullName;

    @Column(nullable = false)
    private String spec;

    @Column(name = "price_krw", nullable = false, precision = 14, scale = 0)
    private BigDecimal priceKrw;

    @Column(nullable = false)
    private boolean deleted;

    @Column(name = "source_lsn", nullable = false)
    private Long sourceLsn;

    @Column(name = "synced_at", nullable = false)
    private OffsetDateTime syncedAt;
}

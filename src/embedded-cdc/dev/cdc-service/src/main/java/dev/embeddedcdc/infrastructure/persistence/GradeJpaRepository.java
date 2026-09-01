package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.GradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * grade 의 Spring Data JPA 저장소. UPSERT 하나뿐이다.
 *
 * ON CONFLICT 가 JPQL 에도 QueryDSL 에도 없어 네이티브로 남는 사정은
 * ComputerJpaRepository 주석과 같다 — 순서 역전 판정이 한 문장 안에서 끝나야 한다.
 */
public interface GradeJpaRepository extends JpaRepository<GradeEntity, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            INSERT INTO grade (id, code, name, discount_rate, created_at, deleted, source_lsn, synced_at)
            VALUES (:id, :code, :name, :discountRate, :createdAt, false, :sourceLsn, now())
            ON CONFLICT (id) DO UPDATE SET
                code          = EXCLUDED.code,
                name          = EXCLUDED.name,
                discount_rate = EXCLUDED.discount_rate,
                created_at    = EXCLUDED.created_at,
                deleted       = EXCLUDED.deleted,
                source_lsn    = EXCLUDED.source_lsn,
                synced_at     = EXCLUDED.synced_at
            WHERE EXCLUDED.source_lsn > grade.source_lsn
            """, nativeQuery = true)
    int upsertIfNewer(@Param("id") long id,
                      @Param("code") String code,
                      @Param("name") String name,
                      @Param("discountRate") BigDecimal discountRate,
                      @Param("createdAt") OffsetDateTime createdAt,
                      @Param("sourceLsn") long sourceLsn);
}

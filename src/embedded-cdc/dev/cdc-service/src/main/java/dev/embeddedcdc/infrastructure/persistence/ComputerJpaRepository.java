package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.ComputerEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;

/**
 * computer 의 Spring Data JPA 저장소. 남은 것은 UPSERT 하나뿐이다.
 *
 * <b>이 문장은 QueryDSL 로도 JPQL 로도 표현할 수 없다.</b>
 * ON CONFLICT 는 JPA 표준에 없고, QueryDSL 의 JPA 모듈도 JPQL 로 번역되는 것만 다루므로
 * 결국 같은 벽에 부딪힌다. 대체 수단인 merge() 는 SELECT 후 INSERT/UPDATE 두 문장이라
 *
 *     WHERE EXCLUDED.source_lsn &gt; computer.source_lsn
 *
 * 이 조건이 "조회해서 비교한 뒤 갱신"으로 벌어지고, 그 사이에 다른 이벤트가 끼어들 수 있다.
 * 순서 역전 방어는 한 문장 안에서 판정되어야 하므로 네이티브로 남긴다.
 *
 * 소프트 삭제는 단순 UPDATE 라 QueryDSL 로 내려갔다 — JpaComputerRepository 를 볼 것.
 */
public interface ComputerJpaRepository extends JpaRepository<ComputerEntity, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            INSERT INTO computer (id, full_name, spec, price_krw, deleted, source_lsn, synced_at)
            VALUES (:id, :fullName, :spec, :priceKrw, false, :sourceLsn, now())
            ON CONFLICT (id) DO UPDATE SET
                full_name  = EXCLUDED.full_name,
                spec       = EXCLUDED.spec,
                price_krw  = EXCLUDED.price_krw,
                deleted    = EXCLUDED.deleted,
                source_lsn = EXCLUDED.source_lsn,
                synced_at  = EXCLUDED.synced_at
            WHERE EXCLUDED.source_lsn > computer.source_lsn
            """, nativeQuery = true)
    int upsertIfNewer(@Param("id") long id,
                      @Param("fullName") String fullName,
                      @Param("spec") String spec,
                      @Param("priceKrw") BigDecimal priceKrw,
                      @Param("sourceLsn") long sourceLsn);
}

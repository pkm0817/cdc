package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.CarEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * car 의 Spring Data JPA 저장소.
 *
 * <b>save() 를 쓰지 않는다.</b> 예전에는 그것 하나였는데, save 는 조건을 걸 수 없어
 * 오래된 이벤트도 그대로 덮었다 — DLQ 재처리가 최신 값을 되돌린 원인이다(V4-b).
 *
 * ON CONFLICT 는 JPA 표준에도 QueryDSL(JPA 모듈)에도 없으므로 네이티브로 남긴다.
 * 대체 수단인 merge() 는 SELECT 후 INSERT/UPDATE 두 문장이라
 *
 *     WHERE EXCLUDED.source_lsn &gt; car.source_lsn
 *
 * 이 조건이 "조회해서 비교한 뒤 갱신"으로 벌어지고 그 사이에 다른 이벤트가 끼어들 수 있다.
 * 순서 역전 방어는 한 문장 안에서 판정되어야 한다 — computer 와 같은 이유다.
 *
 * 삭제는 단순 조건부 DELETE 라 QueryDSL 로 내려갔다(JpaCarRepository).
 * 기본 제공 deleteById 를 쓰지 않는 이유는 findById 로 먼저 로드해 왕복이 두 번이 되기 때문이다.
 */
public interface CarJpaRepository extends JpaRepository<CarEntity, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            INSERT INTO car (id, name, brand, price, created_at, updated_at, source_lsn)
            VALUES (:id, :name, :brand, :price, :createdAt, :updatedAt, :sourceLsn)
            ON CONFLICT (id) DO UPDATE SET
                name       = EXCLUDED.name,
                brand      = EXCLUDED.brand,
                price      = EXCLUDED.price,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                source_lsn = EXCLUDED.source_lsn
            WHERE EXCLUDED.source_lsn > car.source_lsn
            """, nativeQuery = true)
    int upsertIfNewer(@Param("id") long id,
                      @Param("name") String name,
                      @Param("brand") String brand,
                      @Param("price") BigDecimal price,
                      @Param("createdAt") OffsetDateTime createdAt,
                      @Param("updatedAt") OffsetDateTime updatedAt,
                      @Param("sourceLsn") long sourceLsn);
}

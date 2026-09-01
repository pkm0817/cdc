package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.MemberEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;

/**
 * member 의 Spring Data JPA 저장소. UPSERT 하나뿐이다.
 *
 * 충돌 판정 컬럼이 id 뿐이라는 점에 유의할 것. source 의 email 에는 UNIQUE 가 있지만
 * target 에는 걸지 않았다 — 소프트 삭제로 행이 남아 있는 상태에서 같은 이메일이
 * 새 id 로 다시 들어오면 UNIQUE 위반으로 적재가 막히기 때문이다.
 * target 의 유일성은 source 가 이미 보장한 것을 다시 강제하는 것에 지나지 않는다.
 */
public interface MemberJpaRepository extends JpaRepository<MemberEntity, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            INSERT INTO member (id, email, name, grade_id, point, created_at, updated_at,
                                deleted, source_lsn, synced_at)
            VALUES (:id, :email, :name, :gradeId, :point, :createdAt, :updatedAt,
                    false, :sourceLsn, now())
            ON CONFLICT (id) DO UPDATE SET
                email      = EXCLUDED.email,
                name       = EXCLUDED.name,
                grade_id   = EXCLUDED.grade_id,
                point      = EXCLUDED.point,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                deleted    = EXCLUDED.deleted,
                source_lsn = EXCLUDED.source_lsn,
                synced_at  = EXCLUDED.synced_at
            WHERE EXCLUDED.source_lsn > member.source_lsn
            """, nativeQuery = true)
    int upsertIfNewer(@Param("id") long id,
                      @Param("email") String email,
                      @Param("name") String name,
                      @Param("gradeId") long gradeId,
                      @Param("point") int point,
                      @Param("createdAt") OffsetDateTime createdAt,
                      @Param("updatedAt") OffsetDateTime updatedAt,
                      @Param("sourceLsn") long sourceLsn);
}

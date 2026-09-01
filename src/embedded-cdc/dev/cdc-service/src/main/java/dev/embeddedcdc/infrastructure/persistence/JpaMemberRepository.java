package dev.embeddedcdc.infrastructure.persistence;

import com.querydsl.core.types.dsl.DateTimeExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;
import dev.embeddedcdc.domain.model.Member;
import dev.embeddedcdc.domain.port.out.MemberRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

import static dev.embeddedcdc.infrastructure.persistence.entity.QMemberEntity.memberEntity;

/**
 * MemberRepository 의 JPA 구현. JpaComputerRepository 와 같은 형태다.
 *
 * grade 존재 여부를 확인하지 않는다 — 확인해도 할 수 있는 일이 실패시키기뿐이고,
 * target 에 FK 가 없어 DB 도 막지 않는다. 어긋남은 대사(V6)가 잡는다.
 */
@Repository
@Transactional
@RequiredArgsConstructor
public class JpaMemberRepository implements MemberRepository {

    private final MemberJpaRepository jpa;
    private final JPAQueryFactory queryFactory;

    @Override
    public int upsertIfNewer(Member member) {
        return jpa.upsertIfNewer(
                member.id(),
                member.email(),
                member.name(),
                member.gradeId(),
                member.point(),
                member.createdAt(),
                member.updatedAt(),
                member.sourceLsn());
    }

    @Override
    public int softDelete(long id, long lsn) {
        return (int) queryFactory
                .update(memberEntity)
                .set(memberEntity.deleted, true)
                .set(memberEntity.sourceLsn, lsn)
                .set(memberEntity.syncedAt, DateTimeExpression.currentTimestamp(OffsetDateTime.class))
                .where(memberEntity.id.eq(id)
                        .and(memberEntity.sourceLsn.lt(lsn)))
                .execute();
    }
}

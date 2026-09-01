package dev.embeddedcdc.infrastructure.persistence;

import com.querydsl.core.types.dsl.DateTimeExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;
import dev.embeddedcdc.domain.model.Grade;
import dev.embeddedcdc.domain.port.out.GradeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

import static dev.embeddedcdc.infrastructure.persistence.entity.QGradeEntity.gradeEntity;

/**
 * GradeRepository 의 JPA 구현. JpaComputerRepository 와 같은 형태다.
 */
@Repository
@Transactional
@RequiredArgsConstructor
public class JpaGradeRepository implements GradeRepository {

    private final GradeJpaRepository jpa;
    private final JPAQueryFactory queryFactory;

    @Override
    public int upsertIfNewer(Grade grade) {
        return jpa.upsertIfNewer(
                grade.id(),
                grade.code(),
                grade.name(),
                grade.discountRate(),
                grade.createdAt(),
                grade.sourceLsn());
    }

    /**
     * 등급이 사라져도 행은 남긴다 — member.grade_id 가 여전히 이 id 를 가리키고 있어서,
     * 물리 삭제하면 남은 member 의 등급을 되짚을 수 없게 된다.
     */
    @Override
    public int softDelete(long id, long lsn) {
        return (int) queryFactory
                .update(gradeEntity)
                .set(gradeEntity.deleted, true)
                .set(gradeEntity.sourceLsn, lsn)
                .set(gradeEntity.syncedAt, DateTimeExpression.currentTimestamp(OffsetDateTime.class))
                .where(gradeEntity.id.eq(id)
                        .and(gradeEntity.sourceLsn.lt(lsn)))
                .execute();
    }
}

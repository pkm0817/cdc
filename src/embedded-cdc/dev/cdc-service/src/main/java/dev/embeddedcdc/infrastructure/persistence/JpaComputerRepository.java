package dev.embeddedcdc.infrastructure.persistence;

import com.querydsl.core.types.dsl.DateTimeExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;
import dev.embeddedcdc.domain.model.Computer;
import dev.embeddedcdc.domain.port.out.ComputerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

import static dev.embeddedcdc.infrastructure.persistence.entity.QComputerEntity.computerEntity;

/**
 * ComputerRepository 의 JPA 구현.
 *
 * 두 연산 모두 "더 새로운 이벤트일 때만" 반영되며, 그 판정이 한 문장 안에서 끝난다.
 * 반영 행 수 0 은 오류가 아니라 더 오래된 이벤트가 차단된 것이다.
 *
 * upsert 만 네이티브인 이유는 ComputerJpaRepository 주석 참고 —
 * ON CONFLICT 는 JPQL 에도 QueryDSL 에도 없다.
 */
@Repository
@Transactional
@RequiredArgsConstructor
public class JpaComputerRepository implements ComputerRepository {

    private final ComputerJpaRepository jpa;
    private final JPAQueryFactory queryFactory;

    @Override
    public int upsertIfNewer(Computer computer) {
        return jpa.upsertIfNewer(
                computer.id(),
                computer.fullName(),
                computer.spec(),
                computer.priceKrw(),
                computer.sourceLsn());
    }

    /**
     * 물리 삭제가 아니라 플래그만 세운다.
     * 물리 삭제하면 늦게 도착한 UPDATE 가 행을 되살려 유령 데이터가 남는다.
     *
     * synced_at 은 JVM 시각이 아니라 DB 의 current_timestamp 를 쓴다 —
     * 여러 인스턴스가 붙어도 시각 기준이 하나로 유지되어야 지연 관측이 신뢰할 수 있다.
     */
    @Override
    public int softDelete(long id, long lsn) {
        return (int) queryFactory
                .update(computerEntity)
                .set(computerEntity.deleted, true)
                .set(computerEntity.sourceLsn, lsn)
                .set(computerEntity.syncedAt, DateTimeExpression.currentTimestamp(OffsetDateTime.class))
                .where(computerEntity.id.eq(id)
                        .and(computerEntity.sourceLsn.lt(lsn)))
                .execute();
    }
}

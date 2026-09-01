package dev.embeddedcdc.infrastructure.persistence;

import com.querydsl.jpa.impl.JPAQueryFactory;
import dev.embeddedcdc.domain.model.Car;
import dev.embeddedcdc.domain.port.out.CarRepository;
import dev.embeddedcdc.infrastructure.persistence.entity.CarEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import static dev.embeddedcdc.infrastructure.persistence.entity.QCarEntity.carEntity;

/**
 * CarRepository 의 JPA 구현. source 와 스키마가 같아 변환 없이 그대로 저장한다.
 *
 * save() 는 id 가 이미 정해져 있으므로 em.merge() 로 내려간다 —
 * 즉 <b>SELECT 한 번 + INSERT 또는 UPDATE 한 번</b>, 왕복이 두 번이다.
 * 네이티브 UPSERT 한 문장보다 느리지만, car 에는 순서 조건이 없어 정확성은 같다.
 *
 * 삭제는 QueryDSL 벌크 연산이라 영속성 컨텍스트를 거치지 않고 바로 DELETE 문이 나간다.
 * 여기서는 이벤트 한 건이 곧 트랜잭션 하나이고 엔티티를 읽는 경로가 없으므로
 * 컨텍스트와 DB 가 어긋날 여지가 없다.
 *
 * 조건절이 없다는 점에 주의 — 늦게 도착한 오래된 이벤트도 최신 값을 덮어쓴다.
 * 단일 스레드 순차 처리가 그 전제를 지탱하고 있다.
 */
@Repository
@Transactional
@RequiredArgsConstructor
public class JpaCarRepository implements CarRepository {

    private final CarJpaRepository jpa;
    private final JPAQueryFactory queryFactory;

    @Override
    public void upsert(Car car) {
        jpa.save(CarEntity.from(car));
    }

    @Override
    public void delete(long id) {
        queryFactory
                .delete(carEntity)
                .where(carEntity.id.eq(id))
                .execute();
    }
}

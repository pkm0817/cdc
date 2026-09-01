package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.CarEntity;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * car 의 Spring Data JPA 저장소.
 *
 * 메서드가 하나도 없다. 쓰는 것은 상속받은 save() 뿐이고,
 * 삭제는 JpaCarRepository 가 QueryDSL 로 직접 실행한다.
 *
 * 기본 제공 deleteById 를 쓰지 않는 이유는 그쪽이 findById 로 엔티티를 먼저 로드한 뒤
 * remove 하기 때문이다 — 왕복이 두 번이 된다. CDC 처럼 왕복 횟수가 곧 처리량인 경로에서는
 * 한 문장으로 끝내는 편이 낫다.
 */
public interface CarJpaRepository extends JpaRepository<CarEntity, Long> {
}

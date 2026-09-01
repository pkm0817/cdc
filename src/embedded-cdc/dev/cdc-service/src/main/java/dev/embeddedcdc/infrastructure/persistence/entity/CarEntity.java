package dev.embeddedcdc.infrastructure.persistence.entity;

import dev.embeddedcdc.domain.model.Car;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * target 의 car 테이블 매핑.
 *
 * @GeneratedValue 가 없다는 점이 중요하다 — id 는 source 가 발번한 값을 그대로 받는다.
 * target 에는 시퀀스가 없고, 있어서도 안 된다(양쪽 id 가 어긋난다).
 *
 * 도메인의 Car 는 record 라 엔티티가 될 수 없다(JPA 는 기본 생성자와 가변 필드를 요구한다).
 * 그래서 도메인 모델과 엔티티를 분리하고 여기서 변환한다 — 도메인이 JPA 를 모르게 하는 대가다.
 *
 * Lombok 은 @Getter 와 생성자까지만 쓴다. @Data / @EqualsAndHashCode / @ToString 은
 * 엔티티에 붙이지 않는다 — 생성된 equals/hashCode/toString 이 연관 필드를 건드려
 * 지연 로딩을 유발하고, 영속 상태에 따라 동치성이 흔들린다. lombok.config 에서 아예 막아 두었다.
 */
@Entity
@Table(name = "car")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED) // JPA 전용
@AllArgsConstructor(access = AccessLevel.PRIVATE)  // from() 만 쓴다
public class CarEntity {

    @Id
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String brand;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal price;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public static CarEntity from(Car car) {
        return new CarEntity(car.id(), car.name(), car.brand(), car.price(),
                car.createdAt(), car.updatedAt());
    }
}

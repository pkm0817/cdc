package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.Car;

/**
 * target 의 car 저장소(outbound port).
 *
 * upsert 는 반드시 멱등이어야 한다 — 재기동이나 재스냅샷으로 같은 행이 여러 번 도착한다.
 */
public interface CarRepository {

    void upsert(Car car);

    void delete(long id);
}

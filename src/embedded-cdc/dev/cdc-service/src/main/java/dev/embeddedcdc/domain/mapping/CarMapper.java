package dev.embeddedcdc.domain.mapping;

import dev.embeddedcdc.domain.model.Car;
import dev.embeddedcdc.domain.model.RowData;

/**
 * source 의 car 행을 target 의 car 로 옮긴다.
 *
 * 변환 규칙이 없다는 것이 이 클래스의 내용이다. 그래도 클래스를 두는 이유는
 * "source 컬럼 이름을 아는 곳"을 한 군데로 모으기 위해서다 —
 * 컬럼명이 여기저기 흩어지면 source 스키마가 바뀔 때 어디를 고쳐야 하는지 알 수 없다.
 */
public final class CarMapper {

    private CarMapper() {
    }

    public static Car from(RowData row) {
        return new Car(
                row.longValue("id"),
                row.text("name"),
                row.text("brand"),
                row.decimal("price"),
                row.timestamp("created_at"),
                row.timestamp("updated_at"));
    }
}

package dev.embeddedcdc.domain.model;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * target 의 car 한 행. source 와 스키마가 같아 변환이 없다.
 *
 * id 가 source 에서 발번된 값이라는 점만 다르다 — target 에는 시퀀스가 없다.
 *
 * sourceLsn 은 원천에 없는 관리 값이다. 이 행에 마지막으로 반영된 이벤트의 위치이며,
 * 늦게 도착한 이벤트를 한 문장 안에서 걸러내기 위해 도메인까지 올라와 있다.
 * 이 값이 없던 동안 DLQ 재처리가 최신 값을 과거 값으로 되돌렸다(V4-b).
 */
public record Car(
        long id,
        String name,
        String brand,
        BigDecimal price,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        long sourceLsn
) {
}

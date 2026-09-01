package dev.embeddedcdc.domain.model;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * target 의 car 한 행. source 와 스키마가 같아 변환이 없다.
 *
 * id 가 source 에서 발번된 값이라는 점만 다르다 — target 에는 시퀀스가 없다.
 */
public record Car(
        long id,
        String name,
        String brand,
        BigDecimal price,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
}

package dev.embeddedcdc.domain.model;

import java.time.OffsetDateTime;

/**
 * target 의 member 한 행. grade 를 참조하는 자식 테이블이다.
 *
 * <b>gradeId 는 그냥 long 이다 — Grade 참조가 아니다.</b>
 * 이벤트는 테이블마다 따로 오므로 member 이벤트를 받는 시점에 해당 grade 가
 * target 에 있다는 보장이 없다. 객체 그래프를 만들려 들면 그 순간 조회가 필요해지고,
 * 없으면 실패한다. CDC 는 행 단위 복제라 관계는 값(외래 키 컬럼)으로만 옮긴다.
 */
public record Member(
        long id,
        String email,
        String name,
        long gradeId,
        int point,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        long sourceLsn
) {
}

package dev.embeddedcdc.domain.mapping;

import dev.embeddedcdc.domain.model.Grade;
import dev.embeddedcdc.domain.model.RowData;

/**
 * source 의 grade 행을 target 의 grade 로 옮긴다. 변환 규칙은 없다.
 *
 * CarMapper 와 같은 이유로 존재한다 — source 컬럼 이름을 아는 곳을 한 군데로 모은다.
 */
public final class GradeMapper {

    private GradeMapper() {
    }

    public static Grade from(RowData row, long lsn) {
        return new Grade(
                row.longValue("id"),
                row.text("code"),
                row.text("name"),
                row.decimal("discount_rate"),
                row.timestamp("created_at"),
                lsn);
    }
}

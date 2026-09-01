package dev.embeddedcdc.domain.mapping;

import dev.embeddedcdc.domain.model.Member;
import dev.embeddedcdc.domain.model.RowData;

/**
 * source 의 member 행을 target 의 member 로 옮긴다.
 *
 * grade_id 를 grade 조회 없이 그대로 싣는다는 점이 이 매퍼의 내용이다.
 * 예컨대 grade.code 를 붙여 비정규화하려면 여기서 grade 를 조회해야 하는데,
 * 그러면 매퍼가 순수 함수가 아니게 되고 grade 가 아직 안 왔을 때 실패한다.
 * 그런 결합은 CDC 적재가 아니라 하류(뷰·배치)에서 푸는 편이 낫다.
 */
public final class MemberMapper {

    private MemberMapper() {
    }

    public static Member from(RowData row, long lsn) {
        return new Member(
                row.longValue("id"),
                row.text("email"),
                row.text("name"),
                row.longValue("grade_id"),
                row.intValue("point"),
                row.timestamp("created_at"),
                row.timestamp("updated_at"),
                lsn);
    }
}

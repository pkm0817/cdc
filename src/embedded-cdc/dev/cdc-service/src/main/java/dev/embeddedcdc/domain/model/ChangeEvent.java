package dev.embeddedcdc.domain.model;

/**
 * 변경 한 건. 이 파이프라인이 다루는 유일한 입력이다.
 *
 * @param table      변경이 일어난 source 테이블명. 알 수 없는 이름도 그대로 담는다 —
 *                   버릴지 말지는 도메인이 아니라 응용 계층이 판단하고 로그를 남긴다.
 * @param op         변경의 종류
 * @param before     변경 전 행. REPLICA IDENTITY FULL 이라 전체 컬럼이 담긴다. INSERT 는 null
 * @param after      변경 후 행. DELETE 는 null
 * @param lsn        WAL 상 위치. 단조 증가하는 정수라 순서 역전 방어의 기준 키로 쓴다
 * @param sourceTsMs source DB 에서 커밋된 시각(epoch millis). end-to-end 지연 계산용
 */
public record ChangeEvent(
        String table,
        Operation op,
        RowData before,
        RowData after,
        long lsn,
        long sourceTsMs
) {
}

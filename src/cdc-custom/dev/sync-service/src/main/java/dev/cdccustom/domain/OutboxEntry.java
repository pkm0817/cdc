package dev.cdccustom.domain;

import java.time.Instant;

/**
 * outbox 한 줄. "어느 표의 어느 행이 손대졌다"가 전부다.
 *
 * <p>값이 없다는 것이 이 방식의 핵심이다. CDC 의 이벤트는 변경 전·후 이미지를
 * 들고 다니지만(그래서 UPDATE 1건이 1KB 가까이 된다), 여기서는 PK 만 들고
 * 실제 값은 반영 시점에 소스에서 읽는다.
 *
 * @param seq        outbox 순번 (체크포인트가 가리키는 값)
 * @param table      소스 테이블 이름
 * @param rowId      바뀐 행의 PK
 * @param op         연산
 * @param changedAt  트리거가 기록한 시각 — end-to-end 지연 계산의 시작점
 */
public record OutboxEntry(long seq, String table, long rowId, Op op, Instant changedAt) {

    public ChangeKey key() {
        return new ChangeKey(table, rowId);
    }
}

package dev.embeddedcdc.domain.model;

/**
 * 재처리를 기다리는 격리 건.
 *
 * DLQ 에 저장된 payload 로부터 원래 이벤트를 복원한 결과다.
 * 복원이 가능하다는 것이 곧 "격리는 유실이 아니다"의 근거다.
 *
 * @param id       DLQ 행 식별자. 처리 결과를 되돌려 표시할 때 쓴다
 * @param event    복원된 변경 이벤트
 * @param attempts 지금까지 시도한 횟수
 */
public record PendingDeadLetter(long id, ChangeEvent event, int attempts) {
}

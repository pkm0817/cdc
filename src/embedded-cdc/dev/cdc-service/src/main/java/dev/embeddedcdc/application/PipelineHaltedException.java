package dev.embeddedcdc.application;

/**
 * 파이프라인을 계속 돌리면 안 되는 상황.
 *
 * 이 예외가 나가면 Debezium 엔진이 종료되고 오프셋은 전진하지 않는다.
 * 즉 <b>멈춘 지점부터 다시 읽는다</b> — 유실 없이 사람의 개입을 기다리는 상태가 된다.
 */
public class PipelineHaltedException extends RuntimeException {

    public PipelineHaltedException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineHaltedException(String message) {
        super(message);
    }
}

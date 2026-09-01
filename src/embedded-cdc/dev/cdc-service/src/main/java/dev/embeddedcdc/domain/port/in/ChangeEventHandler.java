package dev.embeddedcdc.domain.port.in;

import dev.embeddedcdc.domain.model.ChangeEvent;

import java.util.List;

/**
 * 파이프라인의 입구(inbound port). 변경 <b>배치</b>를 받아 target 에 반영한다.
 *
 * 한 건이 아니라 배치를 받는 이유는 두 가지다.
 *   1. 적용과 진행 지점 기록을 한 트랜잭션으로 묶으려면 경계가 배치여야 한다
 *   2. 이벤트마다 왕복하면 처리량이 왕복 횟수에 묶인다
 *
 * 이 메서드가 예외를 던지면 오프셋이 전진하지 않는다. 즉 던지는 것이 곧 "유실 없이 멈춤"이다.
 */
public interface ChangeEventHandler {

    void handle(String pipeline, List<ChangeEvent> batch);
}

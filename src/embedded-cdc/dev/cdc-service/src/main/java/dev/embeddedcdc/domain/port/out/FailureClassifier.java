package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.FailureVerdict;

/**
 * 적용 실패의 성격을 판정한다.
 *
 * 판정 근거(SQLState 등)는 저장 기술에 딸린 지식이라 구현이 인프라에 있다.
 * 응용 계층은 "재시도할지, 격리할지, 멈출지"만 알면 된다.
 */
public interface FailureClassifier {

    FailureVerdict classify(Throwable cause);
}

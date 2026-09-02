package dev.embeddedcdc.infrastructure.health;

import dev.embeddedcdc.infrastructure.cdc.DebeziumEngineManager;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * 캡처 갭으로 엔진을 띄우지 못한 상태를 health 로 드러낸다.
 *
 * 갭을 발견해도 프로세스는 살려 둔다(그래야 cdc_capture_gap 이 스크랩된다).
 * 그 대가로 up==1 이라 프로세스만 보는 오케스트레이터에겐 정상으로 보인다 —
 * 그 구멍을 메우는 것이 이 지표다. 컨테이너 헬스체크가 /actuator/health 를 보므로
 * 여기서 DOWN 을 내면 "떠 있지만 일하지 않는" 상태가 바깥에 드러난다.
 */
@Component
@RequiredArgsConstructor
public class CdcHealthIndicator implements HealthIndicator {

    private final DebeziumEngineManager engine;

    @Override
    public Health health() {
        String halted = engine.haltedReason();
        return halted == null
                ? Health.up().withDetail("engine", "running").build()
                : Health.down().withDetail("captureGap", halted).build();
    }
}

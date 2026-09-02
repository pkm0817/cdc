package dev.cdccustom.application;

import dev.cdccustom.infrastructure.config.SyncProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 동기화 루프를 돌리는 단 하나의 스레드.
 *
 * <p>스레드를 하나로 두는 것이 의도다. 여러 스레드가 outbox 를 나눠 읽으면 접기 효과가
 * 흩어지고(같은 행이 다른 배치로 갈라진다) 체크포인트 전진도 복잡해진다.
 * 처리량은 스레드 수가 아니라 <b>배치 크기</b>로 올린다 — 한 번에 더 많이 접을수록 빨라진다.
 *
 * <p>{@code @Scheduled} 를 쓰지 않은 이유: 고정 주기로 깨우면 밀려 있을 때도 주기만큼
 * 쉬게 된다. 여기서는 반영할 것이 있는 동안 쉬지 않고 이어서 돈다.
 */
@Component
public class SyncRunner {

    private static final Logger log = LoggerFactory.getLogger(SyncRunner.class);

    private final SyncWorker worker;
    private final SyncProperties props;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread thread;

    // 헬스가 읽는다. 루프가 "돌고는 있지만 매번 실패하는" 상태를 밖에서 구분하기 위한 값이다.
    private volatile int consecutiveFailures;
    private volatile String lastFailure;

    public SyncRunner(SyncWorker worker, SyncProperties props) {
        this.worker = worker;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        thread = new Thread(this::loop, "sync-worker");
        thread.setDaemon(false);
        thread.start();
        log.info("동기화 루프 시작 batchSize={} pollInterval={}ms", props.batchSize(), props.pollIntervalMs());
    }

    private void loop() {
        while (running.get()) {
            try {
                int applied = worker.runOnce();
                consecutiveFailures = 0;
                lastFailure = null;
                if (applied == 0) {
                    sleep(props.pollIntervalMs());
                }
            } catch (Exception e) {
                consecutiveFailures++;
                lastFailure = e.getClass().getSimpleName() + ": " + e.getMessage();
                // 여기서 죽지 않는다. 체크포인트가 전진하지 않았으므로 같은 구간을 다시 읽고,
                // 반영은 UPSERT 라 두 번 들어와도 결과가 같다. 즉 재시도가 안전하다.
                long backoff = Math.min(props.pollIntervalMs() * (1L << Math.min(consecutiveFailures, 6)), 30_000L);
                log.error("동기화 주기 실패 — {}ms 뒤 재시도 (연속 {}회)", backoff, consecutiveFailures, e);
                sleep(backoff);
            }
        }
        log.info("동기화 루프 종료");
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running.set(false);
        }
    }

    public int consecutiveFailures() {
        return consecutiveFailures;
    }

    public String lastFailure() {
        return lastFailure;
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
        }
    }
}

package dev.embeddedcdc.infrastructure.cdc;

import dev.embeddedcdc.domain.port.in.ChangeEventHandler;
import dev.embeddedcdc.domain.port.out.DeadLetterStore;
import dev.embeddedcdc.domain.port.out.PipelineMetrics;
import dev.embeddedcdc.infrastructure.config.CdcSourceProperties;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Debezium Embedded Engine 의 라이프사이클 관리. 파이프라인을 밖에서 밀어 주는 구동 어댑터다.
 *
 * Kafka Connect 없이 이 애플리케이션 안에서 커넥터가 직접 돈다:
 *   - source PostgreSQL 의 WAL 을 logical replication(pgoutput)으로 스트리밍
 *   - offset 은 Kafka topic 대신 로컬 파일(FileOffsetBackingStore)에 저장
 *   - 이벤트는 단일 스레드에서 WAL 순서 그대로 ChangeEventHandler 로 전달
 *
 * 종료 시 반드시 engine.close() 를 먼저 호출한다 — executor 를 먼저 내리면
 * 스레드가 인터럽트되어 offset flush 가 깨질 수 있다 (Debezium 공식 문서 경고).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DebeziumEngineManager implements SmartLifecycle {

    private final CdcSourceProperties props;
    private final DebeziumEventDeserializer deserializer;
    private final ChangeEventHandler handler;
    private final PipelineMetrics metrics;
    private final SlotContinuityGuard continuityGuard;
    private final DeadLetterStore deadLetters;

    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;
    private volatile boolean running;

    /** 캡처 갭으로 엔진을 띄우지 못했을 때의 사유. null 이면 정상이다. */
    private volatile String haltedReason;

    /** health 지표가 읽는다. 프로세스는 살아 있으므로 이걸 안 보면 정상으로 오인된다. */
    public String haltedReason() {
        return haltedReason;
    }

    @Override
    public void start() {
        // 되받을 수 없는 구간이 생겼는데 모르고 기동하면 그 뒤로는 계속 어긋난 채로 돈다.
        // 조용히 도는 것보다 멈추는 편이 낫다 — 재동기화는 사람이 판단할 일이다.
        var gap = continuityGuard.detectGap();
        metrics.captureGap(gap.isPresent());

        if (gap.isPresent() && props.failOnCaptureGap()) {
            // 여기서 예외를 던지면 스프링 컨텍스트가 그대로 죽어 Prometheus 가 한 번도 못 긁는다.
            // 탐지해 놓고 신호를 못 내보내면 탐지한 적 없는 것과 같다.
            // 그래서 엔진만 띄우지 않고 앱은 살려 cdc_capture_gap=1 과 health DOWN 을 노출한다.
            // "조용히 어긋난 채 도는 것보다 멈춘다"는 원래 의도는 그대로다 — 엔진이 안 도니까.
            haltedReason = gap.get().describe();
            // 여기서는 cdc_pipeline_halts_total 을 올리지 않는다. 이 정지는 첫 스크랩
            // 이전에 확정되므로 카운터가 처음부터 1 인 채로 평평하고,
            // increase() 기반 경보가 영원히 안 걸린다 — 있으나 마나 한 신호가 된다.
            // 기동 시 정지는 게이지(cdc_capture_gap)가 맡고, 카운터는 런타임 정지
            // (DLQ_RATIO / UNRECOVERABLE)만 센다. 그쪽은 0 에서 1 로 오르는 것이 보인다.
            log.error("캡처 연결고리 유실 — 엔진을 기동하지 않는다: {}", haltedReason);
            return;
        }
        gap.ifPresent(g -> log.error("캡처 연결고리 유실(기동은 계속함): {}", g.describe()));

        try {
            Path offsetPath = Path.of(props.offsetFile());
            if (offsetPath.getParent() != null) {
                Files.createDirectories(offsetPath.getParent());
            }
        } catch (Exception e) {
            throw new IllegalStateException("offset 파일 디렉터리 생성 실패: " + props.offsetFile(), e);
        }

        engine = DebeziumEngine.create(Json.class)
                .using(engineProperties())
                .notifying(this::handleBatch)
                .using((success, message, error) -> {
                    running = false;
                    if (error != null) {
                        log.error("Debezium engine 종료 (실패): {}", message, error);
                    } else {
                        log.info("Debezium engine 종료: {}", message);
                    }
                })
                .build();

        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "debezium-engine"));
        executor.execute(engine);
        running = true;
        log.info("Debezium embedded engine 시작 — name={} source={}:{}/{} slot={} publication={}",
                props.name(), props.hostname(), props.port(), props.dbname(),
                props.slotName(), props.publicationName());
    }

    /**
     * 배치 하나를 통째로 넘긴다.
     *
     * markProcessed 는 <b>핸들러가 정상 반환한 뒤에만</b> 호출된다.
     * 핸들러가 예외를 던지면 오프셋이 전진하지 않으므로, 다음 기동에서 이 배치를 다시 읽는다.
     * 이전 구현은 예외를 삼켜 실패해도 오프셋이 올라갔고, 그것이 유실의 원인이었다.
     */
    private void handleBatch(List<ChangeEvent<String, String>> records,
                             DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
            throws InterruptedException {
        List<dev.embeddedcdc.domain.model.ChangeEvent> batch = new ArrayList<>(records.size());
        for (ChangeEvent<String, String> record : records) {
            deserialize(record.value()).ifPresent(batch::add);
        }

        handler.handle(props.name(), batch);

        for (ChangeEvent<String, String> record : records) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    /**
     * 역직렬화 실패는 여기서 끊는다 — 깨진 이벤트 하나가 배치 전체를 막지 않는다.
     * 대신 버리지 않고 DLQ 에 원문을 남긴다. 원인을 고친 뒤 다시 처리할 수 있다.
     */
    private java.util.Optional<dev.embeddedcdc.domain.model.ChangeEvent> deserialize(String eventJson) {
        try {
            return deserializer.deserialize(eventJson);
        } catch (Exception e) {
            metrics.applyFailed("unknown");
            deadLetters.storeUnparsable(props.name(), eventJson, e);
            log.error("이벤트 역직렬화 실패, DLQ 로 격리: {}", eventJson, e);
            return java.util.Optional.empty();
        }
    }

    private Properties engineProperties() {
        Properties p = new Properties();
        // ── 엔진 자체 설정 ──────────────────────────────────────────────
        // name 과 topic.prefix 는 커넥터의 정체성이다. 같은 DB 에 커넥터를 둘 이상 붙일 때
        // 이 둘이 겹치면 오프셋 키와 지표 이름이 충돌하므로 설정으로 받는다.
        p.setProperty("name", props.name());
        p.setProperty("topic.prefix", props.topicPrefix());
        p.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        p.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        p.setProperty("offset.storage.file.filename", props.offsetFile());
        p.setProperty("offset.flush.interval.ms", "10000");

        // ── source PostgreSQL 접속 ─────────────────────────────────────
        p.setProperty("database.hostname", props.hostname());
        p.setProperty("database.port", String.valueOf(props.port()));
        p.setProperty("database.user", props.user());
        p.setProperty("database.password", props.password());
        p.setProperty("database.dbname", props.dbname());

        // ── 캡처 방식: 로그 기반 (logical decoding) ─────────────────────
        p.setProperty("plugin.name", "pgoutput");           // PostgreSQL 10+ 내장, 확장 설치 불필요
        p.setProperty("slot.name", props.slotName());
        p.setProperty("publication.name", props.publicationName());
        p.setProperty("publication.autocreate.mode", "disabled"); // init SQL 에서 이미 생성
        p.setProperty("table.include.list", props.tableIncludeList());
        p.setProperty("snapshot.mode", props.snapshotMode());

        // ── 이벤트 포맷 단순화 ─────────────────────────────────────────
        p.setProperty("tombstones.on.delete", "false");     // Kafka 로그 압축용 null 메시지 불필요
        p.setProperty("decimal.handling.mode", "string");   // NUMERIC 을 base64 대신 "1234.56" 문자열로
        p.setProperty("converter.schemas.enable", "false"); // 거대한 schema 블록 제거

        // ── heartbeat ──────────────────────────────────────────────────
        // 관심 테이블에 변경이 없으면 confirmed_flush_lsn 이 전혀 움직이지 않아
        // WAL 이 계속 쌓인다. interval 만으로는 부족하고(V6 측정: 0 bytes),
        // action.query 가 publication 에 든 테이블에 쓰기를 만들어야 전진한다.
        if (props.heartbeatIntervalMs() > 0) {
            p.setProperty("heartbeat.interval.ms", String.valueOf(props.heartbeatIntervalMs()));
            if (props.heartbeatActionQuery() != null && !props.heartbeatActionQuery().isBlank()) {
                p.setProperty("heartbeat.action.query", props.heartbeatActionQuery());
            }
        }
        return p;
    }

    @Override
    public void stop() {
        try {
            if (engine != null) {
                engine.close(); // 남은 이벤트 전달 + offset flush 후 정상 종료
            }
        } catch (Exception e) {
            log.warn("engine close 중 오류", e);
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("engine 스레드가 30초 내에 종료되지 않음");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}

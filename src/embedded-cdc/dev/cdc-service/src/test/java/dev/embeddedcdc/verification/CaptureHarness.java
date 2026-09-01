package dev.embeddedcdc.verification;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.infrastructure.cdc.DebeziumEventDeserializer;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 검증용 캡처 하니스 — Provider 프로세스 하나를 테스트가 마음대로 켜고 끌 수 있게 감싼 것.
 *
 * 운영 코드의 {@link DebeziumEventDeserializer} 를 그대로 쓴다. 테스트 전용 파서를 따로 두면
 * 정작 운영에서 쓰는 파싱 경로가 검증되지 않는다.
 *
 * 엔진 설정은 DebeziumEngineManager 와 같은 값을 쓰되, 슬롯·publication·오프셋 파일만
 * 테스트가 지정한다. 기동 중인 emb-cdc-service 와 슬롯을 다투지 않게 하기 위해서다.
 */
public class CaptureHarness implements AutoCloseable {

    private final DebeziumEventDeserializer deserializer = new DebeziumEventDeserializer();
    private final BlockingQueue<ChangeEvent> events = new LinkedBlockingQueue<>();
    private final AtomicReference<Throwable> engineFailure = new AtomicReference<>();
    private final AtomicReference<String> engineFailureMessage = new AtomicReference<>();

    private final Properties props;
    private DebeziumEngine<io.debezium.engine.ChangeEvent<String, String>> engine;
    private ExecutorService executor;
    private volatile boolean running;

    /**
     * @param slotName     replication slot 이름. 테스트마다 다르게 주어 서로 간섭하지 않게 한다
     * @param publication  이미 만들어진 publication 이름
     * @param tables       캡처 대상 (예: "public.verify_record")
     * @param offsetFile   오프셋 파일 경로. 재기동 검증에서는 같은 경로를 다시 넘긴다
     * @param extra        추가 엔진 설정 (heartbeat 등). 없으면 빈 Properties
     */
    public CaptureHarness(String slotName, String publication, String tables,
                          Path offsetFile, Properties extra) {
        this.props = baseProperties(slotName, publication, tables, offsetFile);
        this.props.putAll(extra);
    }

    private Properties baseProperties(String slotName, String publication, String tables, Path offsetFile) {
        Properties p = new Properties();
        p.setProperty("name", "verify-" + slotName);
        p.setProperty("topic.prefix", "verify_" + slotName);
        p.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        p.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        p.setProperty("offset.storage.file.filename", offsetFile.toAbsolutePath().toString());
        // 운영 기본값 10초는 재기동 검증에서 관측이 어려워 1초로 줄인다.
        // 이 값이 곧 "죽었을 때 되돌아가는 최대 구간"이다.
        p.setProperty("offset.flush.interval.ms", "1000");

        p.setProperty("database.hostname", Db.host());
        p.setProperty("database.port", String.valueOf(Db.port()));
        p.setProperty("database.user", Db.user());
        p.setProperty("database.password", Db.password());
        p.setProperty("database.dbname", Db.database());

        p.setProperty("plugin.name", "pgoutput");
        p.setProperty("slot.name", slotName);
        p.setProperty("publication.name", publication);
        p.setProperty("publication.autocreate.mode", "disabled");
        p.setProperty("table.include.list", tables);
        p.setProperty("snapshot.mode", "no_data"); // 검증은 스트리밍만 본다. 스냅샷은 V3 재동기화에서 따로 다룬다

        p.setProperty("tombstones.on.delete", "false");
        p.setProperty("decimal.handling.mode", "string");
        p.setProperty("converter.schemas.enable", "false");
        return p;
    }

    public CaptureHarness start() {
        try {
            Path parent = Path.of(props.getProperty("offset.storage.file.filename")).getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (Exception e) {
            throw new IllegalStateException("오프셋 디렉터리 생성 실패", e);
        }

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> deserializer.deserialize(record.value()).ifPresent(events::add))
                .using((success, message, error) -> {
                    running = false;
                    engineFailureMessage.set(message);
                    if (error != null) {
                        engineFailure.set(error);
                    }
                })
                .build();

        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "verify-engine"));
        executor.execute(engine);
        running = true;
        return this;
    }

    /**
     * 정상 종료 — 남은 이벤트를 넘기고 오프셋을 flush 한다.
     *
     * 엔진이 이미 실패로 끝난 뒤라면 close() 가 그 예외를 다시 던진다.
     * 그건 종료 실패가 아니라 이미 기록된 기동 실패이므로 삼킨다 —
     * engineFailure() 로 확인할 수 있다.
     */
    public void stopGracefully() {
        try {
            if (engine != null) {
                engine.close();
            }
        } catch (Exception e) {
            if (engineFailure.get() == null) {
                engineFailure.set(e);
            }
        } finally {
            shutdownExecutor();
            running = false;
        }
    }

    /** 오프셋 파일 경로. 재기동 시나리오에서 파일을 되돌리는 데 쓴다. */
    public Path offsetFile() {
        return Path.of(props.getProperty("offset.storage.file.filename"));
    }

    /**
     * 강제 종료 — 프로세스가 kill -9 당한 상황을 흉내 낸다.
     * engine.close() 를 부르지 않으므로 마지막 오프셋 flush 가 일어나지 않는다.
     */
    public void kill() {
        if (executor != null) {
            executor.shutdownNow();
        }
        running = false;
    }

    private void shutdownExecutor() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 이벤트 한 건을 기다린다. 시간 안에 오지 않으면 null. */
    public ChangeEvent poll(Duration timeout) {
        try {
            return events.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** 기대 건수만큼 모으거나 시간이 다 될 때까지 기다린다. 모자라도 모인 만큼 돌려준다. */
    public List<ChangeEvent> collect(int expected, Duration timeout) {
        List<ChangeEvent> collected = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (collected.size() < expected && System.nanoTime() < deadline) {
            ChangeEvent e = poll(Duration.ofMillis(200));
            if (e != null) {
                collected.add(e);
            }
        }
        return collected;
    }

    public int queuedCount() {
        return events.size();
    }

    public void clearQueue() {
        events.clear();
    }

    public boolean isRunning() {
        return running;
    }

    /** 엔진이 실패로 끝났으면 그 예외. 정상이거나 아직 살아 있으면 null. */
    public Throwable engineFailure() {
        return engineFailure.get();
    }

    public String engineFailureMessage() {
        return engineFailureMessage.get();
    }

    @Override
    public void close() {
        if (running) {
            try {
                stopGracefully();
            } catch (Exception ignored) {
                kill();
            }
        }
    }
}

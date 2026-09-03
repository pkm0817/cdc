package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pqcdc "github.com/Trendyol/go-pq-cdc"
	pqconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/config"
	"github.com/prometheus/client_golang/prometheus"
)

// Engine 은 go-pq-cdc 커넥터의 라이프사이클을 관리한다.
// 파이프라인을 밖에서 밀어 주는 구동 어댑터다.
//
// Kafka Connect 도 Debezium 엔진도 없이 이 프로세스 안에서 논리 복제를 직접 읽는다:
//   - source PostgreSQL 의 WAL 을 logical replication(pgoutput)으로 스트리밍
//   - 진행 지점은 파일이 아니라 슬롯의 confirmed_flush_lsn 에 남는다
//   - 이벤트는 라이브러리의 단일 고루틴에서 WAL 순서 그대로 리스너로 전달된다
//
// ── Debezium 판과 결정적으로 다른 점 ─────────────────────────────────────────
// go-pq-cdc 는 이벤트를 한 건씩 넘긴다(배치 API 가 없다). 한 건마다 트랜잭션을 열면
// "적용과 진행 지점 기록이 한 트랜잭션" 이라는 성질은 지켜지지만 왕복이 건수만큼 늘어난다.
// 그래서 리스너는 이벤트를 큐에 넣기만 하고, 별도 고루틴이 다시 배치로 묶어 적용한다.
//
// ack 는 배치가 커밋된 뒤에만, 배치의 마지막 메시지에 대해 한 번 보낸다.
// 라이브러리의 confirmed 위치는 단조 증가하므로 마지막 것 하나면 충분하고,
// 적용에 실패하면 ack 를 아예 보내지 않아 다음 기동에서 그 구간을 다시 읽는다.
// 즉 ack 를 보내지 않는 것이 곧 "유실 없이 멈춤" 이다.
type Engine struct {
	cfg        config.Config
	handler    port.ChangeEventHandler
	guard      *SlotContinuityGuard
	metrics    port.PipelineMetrics
	dlq        port.DeadLetterStore
	collectors []prometheus.Collector
	log        *slog.Logger

	queue chan pending
}

// pending 은 큐에 실리는 한 칸이다.
//
// 관심 밖 메시지(Relation, Commit, 스냅샷 시작·종료 알림 등)도 이벤트 없이 큐에 넣는다.
// 그 자리에서 바로 ack 해 버리면, 앞서 큐에 들어가 아직 적재되지 않은 이벤트를
// 건너뛴 위치까지 confirmed 가 전진해 버린다. 순서를 지키려면 전부 큐를 지나야 한다.
type pending struct {
	event    model.ChangeEvent
	hasEvent bool
	ack      func() error
}

func NewEngine(
	cfg config.Config,
	handler port.ChangeEventHandler,
	guard *SlotContinuityGuard,
	metrics port.PipelineMetrics,
	dlq port.DeadLetterStore,
	collectors []prometheus.Collector,
	log *slog.Logger,
) *Engine {
	return &Engine{
		cfg:        cfg,
		handler:    handler,
		guard:      guard,
		metrics:    metrics,
		dlq:        dlq,
		collectors: collectors,
		log:        log,
		queue:      make(chan pending, cfg.Batch.QueueSize),
	}
}

// Run 은 ctx 가 끝나거나 파이프라인이 멈춰야 할 때까지 블로킹한다.
func (e *Engine) Run(ctx context.Context) error {
	// 되받을 수 없는 구간이 생겼는데 모르고 기동하면 그 뒤로는 계속 어긋난 채로 돈다.
	// 조용히 도는 것보다 기동을 거부하는 편이 낫다 — 재동기화는 사람이 판단할 일이다.
	gap, err := e.guard.DetectGap(ctx)
	if err != nil {
		return err
	}
	// 갭이 없을 때도 0 을 내보낸다. 시계열이 아예 없으면 경보식이
	// "판정한 적 없음" 과 "정상" 을 구분하지 못한다.
	e.metrics.CaptureGap(gap != nil)
	if gap != nil {
		if e.cfg.Source.FailOnCaptureGap {
			return fmt.Errorf("캡처 연결고리 유실: %s", gap.Describe())
		}
		e.log.Error("캡처 연결고리 유실(기동은 계속함)", "gap", gap.Describe())
	}

	connector, err := pqcdc.NewConnector(ctx, e.connectorConfig(), e.listen(ctx))
	if err != nil {
		return fmt.Errorf("커넥터 생성 실패: %w", err)
	}
	// 우리 지표를 라이브러리 레지스트리에 얹는다 — /metrics 하나로 둘 다 나온다.
	connector.SetMetricCollectors(e.collectors...)
	defer connector.Close()

	applyCtx, stopApplying := context.WithCancel(ctx)
	defer stopApplying()

	applyDone := make(chan error, 1)
	go func() { applyDone <- e.applyLoop(applyCtx) }()

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		// Start 는 종료 신호를 받을 때까지 블로킹한다. Close() 가 그 신호 채널을 닫아 준다.
		connector.Start(ctx)
	}()

	e.log.Info("go-pq-cdc 스트림 시작",
		"pipeline", e.cfg.Source.Name,
		"source", fmt.Sprintf("%s:%d/%s", e.cfg.Source.Host, e.cfg.Source.Port, e.cfg.Source.DBName),
		"slot", e.cfg.Source.SlotName,
		"publication", e.cfg.Source.PublicationName)

	select {
	case <-ctx.Done():
		e.log.Info("종료 신호 — 스트림을 내린다")
		return nil

	case applyErr := <-applyDone:
		// 적재가 멈춰야 한다고 판단했다. ack 를 보내지 않았으므로 유실은 없다.
		e.log.Error("적재 루프 정지", "error", applyErr)
		return applyErr

	case <-streamDone:
		// 라이브러리 쪽에서 스트림이 끝났다. 남은 배치를 흘려보낼 방법이 없으므로 같이 내린다.
		stopApplying()
		<-applyDone
		return errors.New("복제 스트림이 종료됐다 — 재기동해 슬롯부터 다시 잡아야 한다")
	}
}

// listen 은 라이브러리가 부르는 리스너다. 큐에 넣기만 하고 적재는 하지 않는다.
//
// 여기서 적재까지 하면 라이브러리의 수신 고루틴이 DB 왕복만큼 멈춰 서고,
// 그동안 WAL 수신이 밀린다. 큐가 그 둘을 떼어 놓는다.
func (e *Engine) listen(ctx context.Context) replication.ListenerFunc {
	return func(lc *replication.ListenerContext) {
		event, relevant, err := Decode(lc.Message)
		if err != nil {
			// 깨진 이벤트 하나가 배치 전체를 막지 않는다. 대신 버리지 않고 DLQ 에 원문을 남긴다.
			e.metrics.ApplyFailed("unknown")
			raw := fmt.Sprintf("%#v", lc.Message)
			if storeErr := e.dlq.StoreUnparsable(ctx, e.cfg.Source.Name, raw, err); storeErr != nil {
				e.log.Error("역직렬화 실패 건을 DLQ 에도 남기지 못했다", "error", storeErr)
			}
			e.log.Error("이벤트 역직렬화 실패, DLQ 로 격리", "error", err)
			e.enqueue(ctx, pending{ack: lc.Ack})
			return
		}
		if !relevant {
			e.enqueue(ctx, pending{ack: lc.Ack})
			return
		}
		e.enqueue(ctx, pending{event: event, hasEvent: true, ack: lc.Ack})
	}
}

// enqueue 는 큐가 가득 차면 막힌다. 그 막힘이 곧 배압이다 —
// 적재가 느리면 WAL 수신도 같이 느려지고, 슬롯이 WAL 을 붙잡아 유실을 막는다.
func (e *Engine) enqueue(ctx context.Context, p pending) {
	select {
	case e.queue <- p:
	case <-ctx.Done():
	}
}

// applyLoop 는 큐에서 꺼낸 것을 배치로 묶어 적용하고, 커밋된 뒤에만 ack 한다.
func (e *Engine) applyLoop(ctx context.Context) error {
	batch := make([]pending, 0, e.cfg.Batch.MaxSize)

	// 타이머는 배치의 첫 칸이 들어온 뒤에만 의미가 있다. 미리 돌려 두면
	// 조용한 구간에서 빈 배치를 계속 만들어 낸다.
	var flushAt <-chan time.Time
	timer := time.NewTimer(e.cfg.Batch.MaxWait)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// 남은 배치는 적용하지 않는다 — 적용하려면 트랜잭션과 ack 가 필요한데,
			// 종료 중에 그 둘을 안전하게 끝냈다고 보장할 수 없다.
			// 적용하지 않았으므로 ack 도 나가지 않고, 다음 기동에서 다시 읽는다.
			return nil

		case p := <-e.queue:
			batch = append(batch, p)
			if len(batch) == 1 {
				timer.Reset(e.cfg.Batch.MaxWait)
				flushAt = timer.C
			}
			if len(batch) < e.cfg.Batch.MaxSize {
				continue
			}
			if !timer.Stop() {
				<-timer.C
			}
			flushAt = nil
			if err := e.flush(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]

		case <-flushAt:
			flushAt = nil
			if err := e.flush(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
}

// flush 는 배치 하나를 적용하고 마지막 메시지에 ack 를 보낸다.
//
// 순서가 중요하다: 적용 → 커밋 → ack. 뒤집으면 ack 한 구간이 적용되지 않은 채
// 슬롯만 전진해 조용한 유실이 된다.
func (e *Engine) flush(ctx context.Context, batch []pending) error {
	if len(batch) == 0 {
		return nil
	}

	events := make([]model.ChangeEvent, 0, len(batch))
	for _, p := range batch {
		if p.hasEvent {
			events = append(events, p.event)
		}
	}

	if len(events) > 0 {
		if err := e.handler.Handle(ctx, e.cfg.Source.Name, events); err != nil {
			return err
		}
	}

	// 마지막 것 하나면 충분하다 — 라이브러리의 confirmed 위치는 단조 증가한다.
	if ack := batch[len(batch)-1].ack; ack != nil {
		if err := ack(); err != nil {
			return fmt.Errorf("ack 실패 — 진행 지점이 전진하지 않는다: %w", err)
		}
	}
	return nil
}

func (e *Engine) connectorConfig() pqconfig.Config {
	src := e.cfg.Source

	cfg := pqconfig.Config{
		Host:     src.Host,
		Port:     src.Port,
		Username: src.User,
		Password: src.Password,
		Database: src.DBName,

		// publication 은 init SQL 이 이미 만들어 두었다. 여기서 만들지 않는 이유는
		// "무엇을 캡처하는가" 의 정의가 애플리케이션 설정이 아니라 DB 스키마에 있어야
		// 모니터링(pg_publication_tables 조회)과 한곳을 보게 되기 때문이다.
		Publication: publication.Config{
			Name:              src.PublicationName,
			CreateIfNotExists: false,
		},

		// 슬롯은 없으면 만든다. 최초 기동에는 만들 수밖에 없기 때문이다.
		// 그 대가로 "슬롯이 사라져도 조용히 새로 만든다"는 위험이 생기는데,
		// 그것을 SlotContinuityGuard 가 기동 시점에 막는다.
		Slot: slot.Config{
			Name:                        src.SlotName,
			CreateIfNotExists:           true,
			SlotActivityCheckerInterval: 3000, // 밀리초 단위 (라이브러리가 time.Millisecond 를 곱한다)
			ProtoVersion:                2,
		},

		Metric: pqconfig.MetricConfig{Port: e.cfg.MetricPort},
		Logger: pqconfig.LoggerConfig{LogLevel: e.cfg.LogLevel},
	}

	if src.SnapshotEnabled {
		cfg.Snapshot = pqconfig.SnapshotConfig{
			Enabled: true,
			Mode:    pqconfig.SnapshotModeInitial,
			Tables:  snapshotTables(src.Tables),
		}
	}

	if src.HeartbeatTable != "" {
		cfg.Heartbeat = pqconfig.HeartbeatConfig{
			Table:    publication.Table{Name: src.HeartbeatTable, Schema: "public"},
			Interval: src.HeartbeatInterval,
		}
	}

	return cfg
}

// snapshotTables 는 스냅샷 대상을 명시한다.
//
// 비워 두면 publication 에 든 표 전부가 대상이 되는데, 거기에는 heartbeat 표도 들어 있다.
// 동기화 대상이 아닌 표를 스냅샷으로 흘려보내면 "매핑되지 않은 테이블" 경고만 쌓인다.
func snapshotTables(tables []string) publication.Tables {
	out := make(publication.Tables, 0, len(tables))
	for _, name := range tables {
		out = append(out, publication.Table{Name: name, Schema: "public"})
	}
	return out
}

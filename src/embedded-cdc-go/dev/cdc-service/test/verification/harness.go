package verification

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	pqcdc "github.com/Trendyol/go-pq-cdc"
	pqconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/cdc"
)

// Captured 는 하니스가 받은 이벤트 한 건과 그 자리의 ack 다.
//
// ack 를 이벤트와 함께 들고 있는 것이 이 하니스의 핵심이다. 운영 코드는
// "적용이 끝난 뒤에만 ack" 로 유실을 막는데, 그 성질을 검증하려면
// 테스트가 ack 시점을 직접 정할 수 있어야 한다 (V4 가 그 위에 서 있다).
type Captured struct {
	Event model.ChangeEvent
	Ack   func() error
}

// HarnessOptions 는 하니스가 붙을 슬롯과 표를 정한다.
type HarnessOptions struct {
	Slot        string
	Publication string
	Tables      []string

	// AutoAck 가 true 면 받는 즉시 ack 한다. 평상시 파이프라인과 같은 상태다.
	// false 면 테스트가 Captured.Ack 를 직접 불러야 슬롯이 전진한다 —
	// "ack 하지 않으면 다시 온다" 를 재현하는 데 쓴다.
	AutoAck bool

	// HeartbeatTable 이 비어 있지 않으면 라이브러리가 그 표에 주기적으로 쓴다.
	// 반드시 Publication 에 들어 있어야 한다 (라이브러리가 기동 시 검증한다).
	HeartbeatTable    string
	HeartbeatInterval time.Duration
}

// Harness 는 검증용 캡처 하니스다 — 커넥터 하나를 테스트가 마음대로 켜고 끌 수 있게 감싼 것.
//
// 운영 코드의 cdc.Decode 를 그대로 쓴다. 테스트 전용 파서를 따로 두면
// 정작 운영에서 쓰는 판독 경로가 검증되지 않는다.
//
// 커넥터 설정도 운영과 같은 값을 쓰되 슬롯·publication·표만 테스트가 지정한다.
// 기동 중인 emb-cdc-go-service 와 슬롯을 다투지 않게 하기 위해서다.
// 스냅샷은 끈다 — 검증은 스트리밍만 본다.
type Harness struct {
	opts      HarnessOptions
	connector pqcdc.Connector
	events    chan Captured

	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	stopped bool
}

// StartHarness 는 하니스를 띄우고 슬롯이 생길 때까지 기다린다.
func StartHarness(opts HarnessOptions) (*Harness, error) {
	if opts.Slot == "" || opts.Publication == "" || len(opts.Tables) == 0 {
		return nil, fmt.Errorf("하니스 설정이 비었다: slot=%q publication=%q tables=%v",
			opts.Slot, opts.Publication, opts.Tables)
	}

	h := &Harness{
		opts: opts,
		// 넉넉히 잡는다. 큐가 막히면 리스너가 멈춰 계측치가 왜곡된다.
		events: make(chan Captured, 100_000),
		done:   make(chan struct{}),
	}

	port, err := freePort()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	connector, err := pqcdc.NewConnector(ctx, h.config(port), h.listen(ctx))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("하니스 커넥터 생성 실패: %w", err)
	}
	h.connector = connector

	go func() {
		defer close(h.done)
		connector.Start(ctx)
	}()

	if err := h.awaitSlot(30 * time.Second); err != nil {
		h.Stop()
		return nil, err
	}
	// 슬롯이 생긴 직후에는 아직 스트리밍에 진입하지 않았을 수 있다.
	// 여기서 여유를 두지 않으면 첫 이벤트를 놓친다.
	time.Sleep(1500 * time.Millisecond)
	return h, nil
}

func (h *Harness) listen(ctx context.Context) replication.ListenerFunc {
	return func(lc *replication.ListenerContext) {
		event, relevant, err := cdc.Decode(lc.Message)
		if err != nil || !relevant {
			// 관심 밖 메시지는 바로 ack 한다. 하니스는 순서 보존을 검증하는 대상이
			// 아니라 이벤트를 관측하는 도구라, 운영 엔진처럼 큐를 태우지 않는다.
			if lc.Ack != nil {
				_ = lc.Ack()
			}
			return
		}
		if h.opts.AutoAck && lc.Ack != nil {
			_ = lc.Ack()
		}
		select {
		case h.events <- Captured{Event: event, Ack: lc.Ack}:
		case <-ctx.Done():
		}
	}
}

func (h *Harness) config(metricPort int) pqconfig.Config {
	conn := sourceConn()

	cfg := pqconfig.Config{
		Host:     conn.host,
		Port:     conn.port,
		Username: conn.user,
		Password: conn.password,
		Database: conn.database,
		Publication: publication.Config{
			Name:              h.opts.Publication,
			CreateIfNotExists: false, // 테스트가 SQL 로 미리 만든다
		},
		Slot: slot.Config{
			Name:                        h.opts.Slot,
			CreateIfNotExists:           true,
			SlotActivityCheckerInterval: 1000,
			ProtoVersion:                2,
		},
		Metric: pqconfig.MetricConfig{Port: metricPort},
		Logger: pqconfig.LoggerConfig{LogLevel: slog.LevelWarn},
	}

	if h.opts.HeartbeatTable != "" {
		interval := h.opts.HeartbeatInterval
		if interval == 0 {
			interval = time.Second
		}
		cfg.Heartbeat = pqconfig.HeartbeatConfig{
			Table:    publication.Table{Name: h.opts.HeartbeatTable, Schema: "public"},
			Interval: interval,
		}
	}
	return cfg
}

func (h *Harness) awaitSlot(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if SlotExists(h.opts.Slot) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("슬롯 %q 이 %s 안에 만들어지지 않았다", h.opts.Slot, timeout)
}

// Stop 은 정상 종료다 — 라이브러리가 마지막 standby status update 를 보낸 뒤 내려간다.
// 즉 ack 한 지점까지가 슬롯에 확정된다.
func (h *Harness) Stop() {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return
	}
	h.stopped = true
	h.mu.Unlock()

	if h.connector != nil {
		h.connector.Close()
	}
	if h.cancel != nil {
		h.cancel()
	}
	select {
	case <-h.done:
	case <-time.After(30 * time.Second):
	}
	// 슬롯 점유가 풀릴 때까지 잠깐 둔다. 바로 다시 붙으면 "in use" 로 튕긴다.
	time.Sleep(500 * time.Millisecond)
}

// Poll 은 이벤트 한 건을 기다린다. 시간 안에 오지 않으면 두 번째 값이 false.
func (h *Harness) Poll(timeout time.Duration) (Captured, bool) {
	select {
	case c := <-h.events:
		return c, true
	case <-time.After(timeout):
		return Captured{}, false
	}
}

// Collect 는 기대 건수만큼 모으거나 시간이 다 될 때까지 기다린다.
// 모자라도 모인 만큼 돌려준다 — 몇 건이 왔는지가 곧 계측치다.
func (h *Harness) Collect(expected int, timeout time.Duration) []Captured {
	collected := make([]Captured, 0, expected)
	deadline := time.Now().Add(timeout)
	for len(collected) < expected && time.Now().Before(deadline) {
		c, ok := h.Poll(200 * time.Millisecond)
		if ok {
			collected = append(collected, c)
		}
	}
	return collected
}

// Drain 은 큐에 남은 것을 버린다. 시나리오 사이를 갈라 놓을 때 쓴다.
func (h *Harness) Drain() {
	for {
		select {
		case <-h.events:
		default:
			return
		}
	}
}

// AckAll 은 받은 것들의 마지막 ack 를 보낸다.
// 라이브러리의 confirmed 위치는 단조 증가하므로 마지막 하나면 충분하다.
func AckAll(captured []Captured) {
	for i := len(captured) - 1; i >= 0; i-- {
		if captured[i].Ack != nil {
			_ = captured[i].Ack()
			return
		}
	}
}

// freePort 는 비어 있는 포트를 하나 고른다.
//
// 하니스마다 커넥터가 자기 HTTP 서버(/metrics, /status)를 띄우기 때문에
// 포트를 고정하면 두 번째 하니스가 뜨지 못한다.
func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("빈 포트를 얻지 못했다: %w", err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

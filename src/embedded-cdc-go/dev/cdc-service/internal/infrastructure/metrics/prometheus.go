// Package metrics 는 파이프라인 관측 지표의 Prometheus 구현이다.
//
// 지표 이름을 Java(Micrometer) 판과 일부러 똑같이 맞췄다. 대시보드 JSON 을 그대로
// 나눠 쓰고, 두 스택을 나란히 띄워 같은 눈금으로 비교하기 위해서다.
// 이름을 바꾸면 Grafana 가 조용히 빈 패널이 된다 — 바꾸려면 대시보드도 같이 고칠 것.
package metrics

import (
	"sync"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
	"github.com/prometheus/client_golang/prometheus"
)

// maxWindow 는 end-to-end 지연의 "최댓값" 이 얼마나 오래 유지되는지다.
//
// Micrometer 의 Timer.max 는 시간이 지나면 잊히는 감쇠 최댓값이다. 그 성질이 없으면
// 한 번 튄 값이 영원히 남아 대시보드가 계속 빨갛다. 여기서는 두 칸짜리 회전 창으로
// 같은 효과를 낸다 — 창이 넘어갈 때마다 이전 칸을 버린다.
const maxWindow = time.Minute

// Prometheus 는 port.PipelineMetrics 의 구현이다.
type Prometheus struct {
	events      *prometheus.CounterVec
	sinkErrors  *prometheus.CounterVec
	deadLetters *prometheus.CounterVec
	lag         *prometheus.SummaryVec
	lagMax      *prometheus.GaugeVec

	mu      sync.Mutex
	windows map[string]*rollingMax
}

func New() *Prometheus {
	return &Prometheus{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cdc_events_total",
			Help: "target 에 반영된 변경 이벤트 수 (op: r=snapshot, c=insert, u=update, d=delete)",
		}, []string{"table", "op"}),
		sinkErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cdc_sink_errors_total",
			Help: "적재에 실패한 이벤트 수",
		}, []string{"table"}),
		deadLetters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cdc_dead_letters_total",
			Help: "DLQ 로 격리된 이벤트 수. 0 이 아니면 미반영 데이터가 쌓이고 있다",
		}, []string{"table"}),
		lag: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "cdc_end_to_end_lag_seconds",
			Help: "source 가 변경을 보낸 시각과 target 반영 시각의 차 (분위수 없이 합계·건수만)",
		}, []string{"table"}),
		lagMax: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cdc_end_to_end_lag_seconds_max",
			Help: "최근 1분 구간의 end-to-end 지연 최댓값",
		}, []string{"table"}),
		windows: make(map[string]*rollingMax),
	}
}

var _ port.PipelineMetrics = (*Prometheus)(nil)

// Collectors 는 go-pq-cdc 가 띄우는 레지스트리에 등록할 수집기 목록이다.
//
// 별도 HTTP 서버를 띄우지 않는 이유가 여기 있다 — 라이브러리가 이미 /metrics 를
// 서비스하고 있으므로, 거기에 얹으면 go_pq_cdc_* 와 cdc_* 가 한 엔드포인트에서 나온다.
// 스크랩 대상이 하나면 "어느 쪽이 안 긁혔지" 를 따질 일이 없다.
func (p *Prometheus) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.events, p.sinkErrors, p.deadLetters, p.lag, p.lagMax}
}

func (p *Prometheus) EventApplied(table, op string) {
	p.events.WithLabelValues(table, op).Inc()
}

func (p *Prometheus) ApplyFailed(table string) {
	p.sinkErrors.WithLabelValues(table).Inc()
}

func (p *Prometheus) DeadLettered(table string) {
	p.deadLetters.WithLabelValues(table).Inc()
}

func (p *Prometheus) EndToEndLag(table string, sourceCommittedAtMs int64) {
	if sourceCommittedAtMs <= 0 {
		return // 커밋 시각이 없는 경우(스냅샷 등)
	}
	lagMs := time.Now().UnixMilli() - sourceCommittedAtMs
	if lagMs < 0 {
		// source DB 와 이 프로세스의 시계가 어긋나면 음수가 나온다. 지연이 아니라 편차다.
		// 0 으로 눌러 담되 버리지는 않는다 — 건수는 세어야 처리량과 대조할 수 있다.
		lagMs = 0
	}
	seconds := float64(lagMs) / 1000

	p.lag.WithLabelValues(table).Observe(seconds)
	p.lagMax.WithLabelValues(table).Set(p.observeMax(table, seconds))
}

func (p *Prometheus) observeMax(table string, value float64) float64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	w, ok := p.windows[table]
	if !ok {
		w = &rollingMax{}
		p.windows[table] = w
	}
	return w.observe(value, time.Now())
}

// rollingMax 는 두 칸짜리 회전 창이다. 창이 넘어가면 이전 칸을 버린다.
type rollingMax struct {
	currentStart time.Time
	current      float64
	previous     float64
}

func (w *rollingMax) observe(value float64, now time.Time) float64 {
	if w.currentStart.IsZero() {
		w.currentStart = now
	}
	switch elapsed := now.Sub(w.currentStart); {
	case elapsed >= 2*maxWindow:
		// 오래 조용했다 — 이전 값은 더 이상 의미가 없다.
		w.previous, w.current, w.currentStart = 0, 0, now
	case elapsed >= maxWindow:
		w.previous, w.current, w.currentStart = w.current, 0, now
	}

	if value > w.current {
		w.current = value
	}
	if w.previous > w.current {
		return w.previous
	}
	return w.current
}

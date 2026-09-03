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
	lagMax      *lagMaxCollector
	captureGap  prometheus.Gauge
	halts       *prometheus.CounterVec
	clockSkew   prometheus.Gauge
	changeAudit *prometheus.CounterVec
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
		lagMax: newLagMaxCollector(),

		// 기동 직후부터 0 이 노출돼야 한다. 갭이 없을 때 시계열이 아예 없으면
		// 경보식(cdc_capture_gap == 1)이 "없음" 과 "정상" 을 구분하지 못한다.
		// Gauge(단일)라 New 시점에 이미 값이 있고, 별도로 열어 줄 필요가 없다.
		captureGap: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cdc_capture_gap",
			Help: "1 이면 기동 시점에 되받을 수 없는 WAL 구간이 발견됐다는 뜻",
		}),
		halts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cdc_pipeline_halts_total",
			Help: "ack 를 보내지 않고 멈춘 횟수 (reason: DLQ_RATIO / UNRECOVERABLE)",
		}, []string{"reason"}),

		// 초 단위로 내보내 end-to-end 지연(초)과 같은 축에서 견줄 수 있게 한다.
		clockSkew: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cdc_clock_skew_seconds",
			Help: "source DB 시계 − 이 프로세스 시계 (초). 지연 수치의 신뢰도 판정에 쓴다",
		}),
		changeAudit: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cdc_change_audit_rows_total",
			Help: "cdc_change_audit 에 남긴 변경 이력 행 수 (필드명은 레이블로 올리지 않는다)",
		}, []string{"table"}),
	}
}

var _ port.PipelineMetrics = (*Prometheus)(nil)

// Collectors 는 go-pq-cdc 가 띄우는 레지스트리에 등록할 수집기 목록이다.
//
// 별도 HTTP 서버를 띄우지 않는 이유가 여기 있다 — 라이브러리가 이미 /metrics 를
// 서비스하고 있으므로, 거기에 얹으면 go_pq_cdc_* 와 cdc_* 가 한 엔드포인트에서 나온다.
// 스크랩 대상이 하나면 "어느 쪽이 안 긁혔지" 를 따질 일이 없다.
func (p *Prometheus) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		p.events, p.sinkErrors, p.deadLetters, p.lag, p.lagMax,
		p.captureGap, p.halts, p.clockSkew, p.changeAudit,

		// 프로세스 수집기(process_start_time_seconds · RSS · fd 수)를 함께 얹는다.
		// 라이브러리 레지스트리에는 Go 런타임 수집기(go_*)만 들어 있어서, 이것이 없으면
		// "앱이 언제 떴는가" 를 알 수 없다. 컨테이너 기동 시각(podman_container_started_seconds)
		// 과는 다른 값이다 — 둘이 어긋나면 컨테이너는 그대로인데 프로세스만 재기동한 것이고,
		// 그게 크래시 루프의 모습이다. Java 판의 process_uptime_seconds 에 대응한다.
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	}
}

func (p *Prometheus) EventApplied(table, op string) {
	p.events.WithLabelValues(table, op).Inc()

	// 지연 지표의 자식을 미리 만들어 둔다. Vec 계열은 라벨 조합이 한 번은 쓰여야
	// 시계열이 생기는데, 스냅샷 행에는 커밋 시각이 없어(아래 EndToEndLag 참고)
	// 최초 적재만으로는 cdc_end_to_end_lag_seconds* 가 하나도 노출되지 않는다.
	// 그러면 기동 직후 Grafana 가 "No data" 를 띄운다 — Debezium 판은 스냅샷 행에도
	// ts_ms 가 실려 오므로 같은 시점에 0 을 보여 준다. 눈금을 맞추려고 여기서 연다.
	p.lag.WithLabelValues(table)
	p.lagMax.track(table)
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
	p.lagMax.observe(table, seconds)
}

func (p *Prometheus) CaptureGap(detected bool) {
	if detected {
		p.captureGap.Set(1)
		return
	}
	p.captureGap.Set(0)
}

func (p *Prometheus) PipelineHalted(reason string) {
	p.halts.WithLabelValues(reason).Inc()
}

// ClockSkew 는 못 잰 경우에 호출되지 않는다 — 마지막으로 잰 값이 그대로 남는다.
// 실패를 0(편차 없음)으로 덮으면 "편차 없음" 과 "못 쟀음" 이 같은 그림이 된다.
func (p *Prometheus) ClockSkew(skewMs int64) {
	p.clockSkew.Set(float64(skewMs) / 1000)
}

func (p *Prometheus) ChangeAudited(table string) {
	p.changeAudit.WithLabelValues(table).Inc()
}

// lagMaxCollector 는 cdc_end_to_end_lag_seconds_max 를 스크랩 시점에 계산해 내보낸다.
//
// GaugeVec 으로는 이 지표를 만들 수 없다. 게이지는 Set 을 불러야 값이 바뀌는데,
// 변경 트래픽이 멎으면 부를 일이 없어 한 번 튄 최댓값이 영원히 남는다.
// (실제로 그랬다 — 부하가 끝난 뒤에도 대시보드의 최대 지연이 내려오지 않았다)
//
// Micrometer 의 Timer.max 는 관측이 없어도 시간이 지나면 0 으로 내려간다.
// 같은 성질을 내려면 "쓸 때" 가 아니라 "읽을 때" 기준으로 창을 굴려야 한다.
// 그래서 Collector 를 직접 구현해 Collect 안에서 현재 시각으로 창을 정리한다.
type lagMaxCollector struct {
	desc *prometheus.Desc

	mu      sync.Mutex
	windows map[string]*rollingMax
}

func newLagMaxCollector() *lagMaxCollector {
	return &lagMaxCollector{
		desc: prometheus.NewDesc(
			"cdc_end_to_end_lag_seconds_max",
			"최근 1분 구간의 end-to-end 지연 최댓값 (관측이 끊기면 0 으로 내려간다)",
			[]string{"table"}, nil,
		),
		windows: make(map[string]*rollingMax),
	}
}

func (c *lagMaxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *lagMaxCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for table, w := range c.windows {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, w.valueAt(now), table)
	}
}

// track 은 값 없이 시계열만 연다. 관측이 오기 전에는 0 이 나간다.
func (c *lagMaxCollector) track(table string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.window(table)
}

func (c *lagMaxCollector) observe(table string, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.window(table).observe(value, time.Now())
}

// window 는 호출자가 c.mu 를 쥐고 있다고 가정한다.
func (c *lagMaxCollector) window(table string) *rollingMax {
	w, ok := c.windows[table]
	if !ok {
		w = &rollingMax{}
		c.windows[table] = w
	}
	return w
}

// rollingMax 는 두 칸짜리 회전 창이다. 창이 넘어가면 이전 칸을 버린다.
type rollingMax struct {
	currentStart time.Time
	current      float64
	previous     float64
}

func (w *rollingMax) observe(value float64, now time.Time) {
	w.roll(now)
	if value > w.current {
		w.current = value
	}
}

// valueAt 은 읽는 시각 기준으로 창을 정리한 뒤 최댓값을 준다.
// 관측이 두 창을 넘도록 없으면 0 이 된다 — 이 정리를 여기서 해야 감쇠가 성립한다.
func (w *rollingMax) valueAt(now time.Time) float64 {
	w.roll(now)
	if w.previous > w.current {
		return w.previous
	}
	return w.current
}

func (w *rollingMax) roll(now time.Time) {
	if w.currentStart.IsZero() {
		w.currentStart = now
		return
	}
	switch elapsed := now.Sub(w.currentStart); {
	case elapsed >= 2*maxWindow:
		// 오래 조용했다 — 이전 값은 더 이상 의미가 없다.
		w.previous, w.current, w.currentStart = 0, 0, now
	case elapsed >= maxWindow:
		w.previous, w.current, w.currentStart = w.current, 0, now
	}
}

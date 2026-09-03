package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
	"github.com/jackc/pgx/v5"
)

// ClockSkewProbe 는 source DB 와 이 프로세스의 시계 차이를 주기적으로 잰다.
//
// 왜 필요한가. cdc_end_to_end_lag_seconds 는 source 이벤트의 커밋 시각(DB 시계)과
// 적재 시각(앱 시계)의 차다. 두 시계가 어긋나면 그 차이가 지연 수치에 통째로 섞인다.
// Java 판 V1 1차 실행이 그랬다 — 편차 3,561ms 에 지연 526ms 였으니 재려던 값보다
// 오차가 7배 컸다. 편차를 같이 노출해야 "지연이 큰 것" 과 "시계가 틀린 것" 을 가른다.
//
// 왜 별도 커넥션인가. 적재 풀은 target 을 가리킨다. 여기서 알아야 할 것은 source
// 시계이므로 매번 짧게 열고 닫는다. 30초에 한 번이라 풀을 하나 더 두는 것보다 싸고,
// 실패해도 파이프라인에 영향이 없다.
//
// 왕복 시간 보정. 질의 직전·직후의 로컬 시각 중간값을 DB 응답 시각과 견준다.
// 보정하지 않으면 네트워크 왕복의 절반이 편차로 계상되어, 편차가 없는 환경에서도
// 수 ms 가 계속 찍힌다.
type ClockSkewProbe struct {
	dsn      string
	interval time.Duration
	metrics  port.PipelineMetrics
	log      *slog.Logger
}

func NewClockSkewProbe(
	dsn string, interval time.Duration, metrics port.PipelineMetrics, log *slog.Logger,
) *ClockSkewProbe {
	return &ClockSkewProbe{dsn: dsn, interval: interval, metrics: metrics, log: log}
}

// Run 은 ctx 가 끝날 때까지 주기적으로 잰다. 고루틴으로 띄운다.
//
// 첫 측정도 한 주기를 기다린 뒤에 한다 — 기동 직후에는 스냅샷 적재가 커넥션을
// 다투고 있어, 그때 잰 왕복 시간은 시계 편차가 아니라 혼잡의 값이다.
func (p *ClockSkewProbe) Run(ctx context.Context) {
	if p.interval <= 0 {
		p.log.Info("시계 편차 측정 꺼짐 — 지연 지표의 신뢰도를 판정할 수 없다")
		return
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.probe(ctx)
		}
	}
}

func (p *ClockSkewProbe) probe(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(probeCtx, p.dsn)
	if err != nil {
		// 못 잰 것을 0 으로 덮지 않는다. 마지막 값이 남고, 못 재고 있다는 사실은 이 로그로 드러난다.
		p.log.Warn("시계 편차 측정 실패 — 지연 지표의 신뢰도를 판정할 수 없다", "error", err)
		return
	}
	defer func() { _ = conn.Close(context.WithoutCancel(probeCtx)) }()

	before := time.Now().UnixMilli()
	var dbNowMs int64
	if err := conn.QueryRow(probeCtx,
		`SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint`).Scan(&dbNowMs); err != nil {
		p.log.Warn("시계 편차 측정 실패 — 지연 지표의 신뢰도를 판정할 수 없다", "error", err)
		return
	}
	after := time.Now().UnixMilli()

	skewMs := dbNowMs - (before+after)/2
	p.metrics.ClockSkew(skewMs)

	// 0.5초는 목표 지연(5초)의 10% 다. 이 선을 넘으면 그 구간의 지연 수치는 판정에 쓰지 않는다.
	if skewMs > 500 || skewMs < -500 {
		p.log.Warn("source DB 와 시계 편차가 크다 — 이 구간의 end-to-end 지연 수치는 신뢰할 수 없다",
			"skewMs", skewMs)
	}
}

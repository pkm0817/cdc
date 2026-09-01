package application

import (
	"context"
	"log/slog"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// DeadLetterPolicy 는 DLQ 재처리 정책이다.
//
//	Enabled   재처리 루프 사용 여부
//	Interval  재처리 주기. RETRY_REQUESTED 가 없으면 아무 일도 하지 않는다
//	BatchSize 한 번에 집는 건수. 너무 크면 정상 파이프라인과 DB 를 다툰다
type DeadLetterPolicy struct {
	Enabled   bool
	Interval  time.Duration
	BatchSize int
}

// DeadLetterReprocessor 는 격리된 이벤트를 다시 반영한다.
//
// PENDING 을 자동으로 집지 않는다. 원인이 고쳐졌는지는 사람만 안다 —
// 자동 재시도를 돌리면 고쳐지지 않은 독성 건이 영원히 재시도되며 잡음만 쌓인다.
// 운영자가 원인을 고친 뒤 상태를 바꾸는 것이 곧 재처리 신청이다.
//
//	UPDATE cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = 1;
//
// 재처리가 안전한 이유는 LSN 가드에 있다. 격리된 뒤 같은 행에 더 새로운 변경이
// 이미 반영됐다면, 오래된 LSN 을 든 이 이벤트는 갱신 행 수 0 으로 차단된다.
// 즉 순서가 뒤바뀐 재처리가 최신 값을 덮어쓰지 않는다.
//
// 다만 그 가드는 computer·grade·member 에만 있다 — car 는 조건 없는 UPSERT 라
// 재처리가 더 새로운 값을 덮어쓸 수 있다. car 를 재처리할 때는 그 사이 변경이
// 없었는지 확인해야 한다.
type DeadLetterReprocessor struct {
	deadLetters port.DeadLetterStore
	applier     *BatchApplier
	pipeline    string
	policy      DeadLetterPolicy
	log         *slog.Logger
}

func NewDeadLetterReprocessor(
	deadLetters port.DeadLetterStore,
	applier *BatchApplier,
	pipeline string,
	policy DeadLetterPolicy,
	log *slog.Logger,
) *DeadLetterReprocessor {
	return &DeadLetterReprocessor{
		deadLetters: deadLetters,
		applier:     applier,
		pipeline:    pipeline,
		policy:      policy,
		log:         log,
	}
}

// Run 은 ctx 가 끝날 때까지 주기적으로 재처리한다. 별도 고루틴에서 부른다.
func (r *DeadLetterReprocessor) Run(ctx context.Context) {
	if !r.policy.Enabled {
		r.log.Info("DLQ 재처리 비활성")
		return
	}
	ticker := time.NewTicker(r.policy.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("DLQ 재처리 루프 종료")
			return
		case <-ticker.C:
			r.reprocessOnce(ctx)
		}
	}
}

func (r *DeadLetterReprocessor) reprocessOnce(ctx context.Context) {
	claimed, err := r.deadLetters.ClaimForRetry(ctx, r.pipeline, r.policy.BatchSize)
	if err != nil {
		r.log.Warn("DLQ 재처리 대상 조회 실패", "error", err)
		return
	}
	if len(claimed) == 0 {
		return
	}

	r.log.Info("DLQ 재처리 시작", "count", len(claimed))
	resolved, failed := 0, 0

	for _, pending := range claimed {
		// 체크포인트를 올리지 않는 경로다 — 지나간 LSN 을 다시 적용하는 것이므로
		// 진행 지점을 건드리면 안 된다.
		applyErr := r.applier.ApplyOne(ctx, pending.Event)
		if applyErr == nil {
			if err := r.deadLetters.MarkResolved(ctx, pending.ID); err != nil {
				r.log.Warn("재처리 성공 표시 실패", "dlqId", pending.ID, "error", err)
			}
			resolved++
			r.log.Info("재처리 성공",
				"dlqId", pending.ID, "table", pending.Event.Table,
				"op", pending.Event.Op.Code(), "lsn", pending.Event.LSN)
			continue
		}

		if err := r.deadLetters.MarkRetryFailed(ctx, pending.ID, applyErr); err != nil {
			r.log.Warn("재처리 실패 표시 실패", "dlqId", pending.ID, "error", err)
		}
		failed++
		r.log.Warn("재처리 실패 — PENDING 으로 되돌림",
			"dlqId", pending.ID, "table", pending.Event.Table,
			"lsn", pending.Event.LSN, "cause", applyErr)
	}

	r.log.Info("DLQ 재처리 완료", "resolved", resolved, "failed", failed)
}

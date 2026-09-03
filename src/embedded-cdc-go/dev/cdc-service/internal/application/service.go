package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// ApplyPolicy 는 적용 실패를 다루는 정책값이다.
//
//	MaxBatchRetries       배치 전체를 다시 시도하는 횟수. 소진하면 건 단위 격리로 넘어간다
//	RetryBackoff          첫 재시도 대기. 시도마다 두 배로 늘어난다
//	HaltOnDeadLetterRatio 한 배치에서 이 비율을 넘게 격리되면 구조 문제로 보고 멈춘다.
//	                      0.5 면 절반이다. 낮출수록 보수적이다
type ApplyPolicy struct {
	MaxBatchRetries       int
	RetryBackoff          time.Duration
	HaltOnDeadLetterRatio float64
}

// ChangeEventService 는 배치 하나를 끝까지 책임진다 — 재시도, 격리, 정지 판단.
//
// 실패를 세 갈래로 나눈다.
//
//	RETRY       배치 전체를 백오프 후 다시 시도한다
//	DEAD_LETTER 건 단위로 좁혀 범인만 격리하고 나머지는 반영한다
//	HALT        오류를 밖으로 돌려준다. ack 가 나가지 않아 유실이 없다
//
// 격리 비율이 임계를 넘으면 데이터 문제가 아니라 구조 문제로 보고 멈춘다 —
// 그러지 않으면 수신 테이블이 통째로 사라졌을 때 DLQ 가 전체 트래픽을 삼킨다.
type ChangeEventService struct {
	applier     *BatchApplier
	classifier  port.FailureClassifier
	deadLetters port.DeadLetterStore
	metrics     port.PipelineMetrics
	policy      ApplyPolicy
	log         *slog.Logger
}

func NewChangeEventService(
	applier *BatchApplier,
	classifier port.FailureClassifier,
	deadLetters port.DeadLetterStore,
	metrics port.PipelineMetrics,
	policy ApplyPolicy,
	log *slog.Logger,
) *ChangeEventService {
	return &ChangeEventService{
		applier:     applier,
		classifier:  classifier,
		deadLetters: deadLetters,
		metrics:     metrics,
		policy:      policy,
		log:         log,
	}
}

var _ port.ChangeEventHandler = (*ChangeEventService)(nil)

func (s *ChangeEventService) Handle(ctx context.Context, pipeline string, batch []model.ChangeEvent) error {
	if len(batch) == 0 {
		return nil
	}

	err := s.applier.ApplyAll(ctx, pipeline, batch)
	if err == nil {
		s.recordSuccess(batch)
		return nil
	}
	if haltErr := s.haltIfUnrecoverable(err); haltErr != nil {
		return haltErr
	}
	s.log.Warn("배치 적용 실패 — 재시도로 넘어간다", "size", len(batch), "error", err)

	retried, haltErr := s.retryBatch(ctx, pipeline, batch)
	if haltErr != nil {
		return haltErr
	}
	if retried {
		return nil
	}

	// 여기까지 왔으면 배치 전체 재시도로는 풀리지 않는다. 범인을 찾아야 한다.
	return s.isolate(ctx, pipeline, batch)
}

// retryBatch 는 배치 전체를 백오프하며 다시 시도한다. 성공하면 첫 값이 true.
func (s *ChangeEventService) retryBatch(
	ctx context.Context, pipeline string, batch []model.ChangeEvent,
) (bool, error) {
	for attempt := 1; attempt <= s.policy.MaxBatchRetries; attempt++ {
		if err := s.backoff(ctx, attempt); err != nil {
			return false, err
		}
		err := s.applier.ApplyAll(ctx, pipeline, batch)
		if err == nil {
			s.recordSuccess(batch)
			s.log.Info("배치 재시도 성공", "attempt", attempt, "size", len(batch))
			return true, nil
		}
		if haltErr := s.haltIfUnrecoverable(err); haltErr != nil {
			return false, haltErr
		}
		s.log.Warn("배치 재시도 실패",
			"attempt", attempt, "max", s.policy.MaxBatchRetries, "error", err)
	}
	return false, nil
}

type failure struct {
	event model.ChangeEvent
	cause error
}

// isolate 는 건 단위로 적용하며 실패한 것만 격리한다.
// 여기까지 온 이벤트는 배치 재시도를 이미 소진했으므로, 일시 장애도 격리 대상이 된다.
func (s *ChangeEventService) isolate(ctx context.Context, pipeline string, batch []model.ChangeEvent) error {
	var (
		failures   []failure
		highestLSN uint64
	)

	for _, event := range batch {
		err := s.applier.ApplyOne(ctx, pipeline, event)
		if err != nil {
			if haltErr := s.haltIfUnrecoverable(err); haltErr != nil {
				return haltErr
			}
			failures = append(failures, failure{event: event, cause: err})
		} else {
			s.metrics.EventApplied(event.Table, event.Op.Code())
			s.metrics.EndToEndLag(event.Table, event.SourceTsMs)
		}
		if event.LSN > highestLSN {
			highestLSN = event.LSN
		}
	}

	// 정지 판단이 DLQ 기록보다 먼저다.
	// 멈출 상황이면 애초에 개별 데이터 문제가 아니므로 격리해서는 안 된다 —
	// 기록해 두면 재기동 후 정상 적용된 뒤에도 DLQ 에 남아 회계가 어긋난다.
	ratio := float64(len(failures)) / float64(len(batch))
	if ratio > s.policy.HaltOnDeadLetterRatio {
		return s.halted(HaltDLQRatio, fmt.Sprintf(
			"격리 비율이 임계를 넘었다 (%d/%d = %.0f%%, 임계 %.0f%%). "+
				"개별 데이터 문제가 아니라 구조 문제로 본다. ack 를 보내지 않고 멈춘다",
			len(failures), len(batch), ratio*100, s.policy.HaltOnDeadLetterRatio*100), nil)
	}

	for _, f := range failures {
		if err := s.deadLetters.Store(ctx, pipeline, f.event, f.cause, s.policy.MaxBatchRetries+1); err != nil {
			// 격리 기록조차 실패하면 그것은 추적되지 않는 유실이다. 멈추는 편이 낫다.
			return s.halted(HaltUnrecoverable, "격리 기록에 실패했다 — 추적되지 않는 유실이 되므로 멈춘다", err)
		}
		s.metrics.DeadLettered(f.event.Table)
		s.metrics.ApplyFailed(f.event.Table)
		s.log.Error("격리",
			"table", f.event.Table, "op", f.event.Op.Code(),
			"lsn", f.event.LSN, "cause", f.cause)
	}

	// 격리된 건도 DLQ 에 남아 추적되므로, 여기까지 왔으면 진행 지점을 올려도 안전하다.
	if highestLSN > 0 {
		if err := s.applier.RecordCheckpoint(ctx, pipeline, highestLSN); err != nil {
			return s.halted(HaltUnrecoverable, "진행 지점 기록에 실패했다", err)
		}
	}
	if len(failures) > 0 {
		s.log.Warn("배치 일부 격리 — DLQ 에서 원인 확인 후 재처리할 것",
			"size", len(batch), "isolated", len(failures))
	}
	return nil
}

// haltIfUnrecoverable 은 멈춰야 하는 실패면 HaltError 를, 아니면 nil 을 돌려준다.
func (s *ChangeEventService) haltIfUnrecoverable(err error) error {
	var halted *HaltError
	if errors.As(err, &halted) {
		return halted
	}
	if s.classifier.Classify(err) == model.VerdictHalt {
		return s.halted(HaltUnrecoverable, "계속 돌리면 안 되는 실패다. ack 를 보내지 않고 멈춘다", err)
	}
	return nil
}

// halted 는 정지 오류를 만들면서 지표에 한 번 센다.
//
// 만드는 곳과 세는 곳을 하나로 묶은 이유가 있다. 정지는 "유실 없이 멈춘" 사건이라
// 로그로만 남기면 지나간 뒤에는 몇 번이었는지 알 수 없다. 둘이 갈라져 있으면
// 새 정지 경로가 생겼을 때 세는 것을 빠뜨린다.
//
// 종료 중 취소(HaltShutdown)는 세지 않는다 — 컨테이너를 내릴 때마다 오르면
// 그 지표로 사고를 구분할 수 없다.
func (s *ChangeEventService) halted(code, reason string, cause error) *HaltError {
	if code != HaltShutdown {
		s.metrics.PipelineHalted(code)
	}
	return halt(code, reason, cause)
}

func (s *ChangeEventService) recordSuccess(batch []model.ChangeEvent) {
	for _, event := range batch {
		s.metrics.EventApplied(event.Table, event.Op.Code())
		s.metrics.EndToEndLag(event.Table, event.SourceTsMs)
	}
}

func (s *ChangeEventService) backoff(ctx context.Context, attempt int) error {
	wait := time.Duration(float64(s.policy.RetryBackoff) * math.Pow(2, float64(attempt-1)))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return s.halted(HaltShutdown, "재시도 대기 중 취소 — 종료 중으로 본다", ctx.Err())
	}
}

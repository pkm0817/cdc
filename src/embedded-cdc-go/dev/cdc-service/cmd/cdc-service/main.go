// cdc-service 는 source PostgreSQL 의 WAL 을 읽어 target PostgreSQL 에 반영한다.
//
// Kafka 도 Kafka Connect 도 없다. go-pq-cdc 가 이 프로세스 안에서 논리 복제를
// 직접 읽고, 우리 코드가 그것을 도메인 이벤트로 바꿔 배치로 적재한다.
//
// 이 파일이 하는 일은 조립뿐이다. 어떤 구현을 어떤 포트에 꽂는지가 한눈에 보여야 하므로
// DI 프레임워크를 쓰지 않았다 — 의존 관계가 여기 적힌 순서 그대로다.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/application"
	"github.com/embedded-cdc-go/cdc-service/internal/application/handler"
	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/cdc"
	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/config"
	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/metrics"
	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/persistence"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		slog.Error("cdc-service 종료", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	// SIGTERM/SIGINT 를 받으면 ctx 가 끝난다. 컨테이너 종료가 이 경로를 탄다.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := openTarget(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer pool.Close()

	// ── 어댑터 ──────────────────────────────────────────────────────────────
	store := persistence.NewStore(pool)
	cars := persistence.NewCarRepository(store)
	computers := persistence.NewComputerRepository(store)
	grades := persistence.NewGradeRepository(store)
	members := persistence.NewMemberRepository(store)
	checkpoints := persistence.NewCheckpointRepository(store)
	deadLetters := persistence.NewDeadLetterRepository(store, log)
	classifier := cdc.NewSQLStateClassifier(log)
	pipelineMetrics := metrics.New()
	changeAudit := persistence.NewChangeAuditRepository(
		store, cfg.Audit.ChangedFieldsTables, pipelineMetrics)

	// ── 응용 ────────────────────────────────────────────────────────────────
	applier, err := application.NewBatchApplier([]handler.TableSyncHandler{
		handler.NewCar(cars),
		handler.NewComputer(computers, log),
		handler.NewGrade(grades, log),
		handler.NewMember(members, log),
	}, checkpoints, changeAudit, store, log)
	if err != nil {
		return err
	}

	service := application.NewChangeEventService(
		applier, classifier, deadLetters, pipelineMetrics,
		application.ApplyPolicy{
			MaxBatchRetries:       cfg.Apply.MaxBatchRetries,
			RetryBackoff:          cfg.Apply.RetryBackoff,
			HaltOnDeadLetterRatio: cfg.Apply.HaltOnDeadLetterRatio,
		}, log)

	reprocessor := application.NewDeadLetterReprocessor(
		deadLetters, applier, cfg.Source.Name,
		application.DeadLetterPolicy{
			Enabled:   cfg.DeadLetter.ReprocessEnabled,
			Interval:  cfg.DeadLetter.ReprocessInterval,
			BatchSize: cfg.DeadLetter.ReprocessBatch,
		}, log)
	go reprocessor.Run(ctx)

	// 시계 편차 프로브. 파이프라인과 독립이라 실패해도 적재에 영향이 없다 —
	// 대신 그 구간의 지연 수치를 판정에 쓸 수 없다는 사실이 지표로 드러난다.
	go metrics.NewClockSkewProbe(
		cfg.SourceDSN(), cfg.Source.ClockSkewProbeInterval, pipelineMetrics, log).Run(ctx)

	// ── 구동 ────────────────────────────────────────────────────────────────
	guard := cdc.NewSlotContinuityGuard(
		cfg.SourceDSN(), cfg.Source.SlotName, cfg.Source.Name, checkpoints, log)

	engine := cdc.NewEngine(
		cfg, service, guard, pipelineMetrics, deadLetters,
		pipelineMetrics.Collectors(), log)

	return engine.Run(ctx)
}

// openTarget 은 적재 대상 풀을 연다.
//
// 기동 시점에 한 번 실제로 붙어 본다. 붙지 못하는데 스트림부터 열면 슬롯을 잡은 채
// 모든 배치가 실패하고, 그 사이 원천에는 WAL 이 쌓인다. 붙을 수 있을 때 시작하는 편이 낫다.
func openTarget(ctx context.Context, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.TargetDSN())
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = cfg.Target.MaxConns

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}

	log.Info("target DB 접속",
		"host", cfg.Target.Host, "port", cfg.Target.Port, "db", cfg.Target.DBName)
	return pool, nil
}

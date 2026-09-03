// Package config 는 환경변수에서 설정을 읽는다.
//
// Java 판은 application.yml + @ConfigurationProperties 였다. Go 에는 그 자리를
// 차지하는 표준이 없으므로 환경변수 하나로 통일했다 — 컨테이너에서 주는 값과
// 로컬에서 주는 값이 같은 경로를 타야 "로컬에서는 되는데" 를 피할 수 있다.
// 기본값은 전부 이 스택의 compose 포트에 맞춰 두어, 아무것도 주지 않아도 돈다.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 는 서비스 전체 설정이다.
type Config struct {
	Source     Source
	Target     Target
	Apply      Apply
	DeadLetter DeadLetter
	Batch      Batch
	Audit      Audit
	MetricPort int
	LogLevel   slog.Level
}

// Source 는 source PostgreSQL 접속과 캡처 범위다.
//
// Name / SlotName / PublicationName 은 인스턴스마다 달라야 하는 값이라 설정으로 뺐다 —
// 같은 DB 에 커넥터를 둘 이상 붙일 때 이 값이 겹치면 슬롯을 다투거나 진행 지점을 서로 덮어쓴다.
type Source struct {
	Name            string // 파이프라인 식별자. 체크포인트·DLQ 의 pipeline 컬럼에 들어간다
	Host            string
	Port            int
	User            string
	Password        string
	DBName          string
	SlotName        string
	PublicationName string
	Tables          []string // 캡처 대상 (schema 없이 테이블 이름만)

	// SnapshotEnabled 가 켜져 있으면 최초 기동 시 전량을 한 번 읽어 target 을 채운다.
	// 켜려면 원천 DB 에 cdc_snapshot_job / cdc_snapshot_chunks 를 만들 권한이 필요하다
	// (go-pq-cdc 가 스냅샷 진행 상황을 원천에 남긴다). 원천을 건드릴 수 없으면 끄고
	// 초기 적재를 따로 해야 한다.
	SnapshotEnabled bool

	// HeartbeatTable 이 비어 있지 않으면 라이브러리가 그 표에 주기적으로 써서
	// 관심 테이블에 변경이 없는 구간에도 슬롯을 전진시킨다. 비워 두면 유휴 구간에
	// WAL 이 계속 쌓인다 (V6 에서 실측).
	HeartbeatTable    string
	HeartbeatInterval time.Duration

	// FailOnCaptureGap 이 false 면 되받을 수 없는 구간을 발견해도 로그만 남기고 계속 돈다 —
	// 어긋난 채로 도는 것을 감수하겠다는 뜻이다.
	FailOnCaptureGap bool

	// ClockSkewProbeInterval 은 source DB 시계와 이 프로세스 시계의 차를 재는 주기다.
	// 0 이면 재지 않는다 — 그 경우 end-to-end 지연이 진짜 지연인지 시계 오차인지
	// 가릴 근거가 사라진다 (cdc_clock_skew_seconds 가 안 나온다).
	ClockSkewProbeInterval time.Duration
}

// Audit 은 변경 이력 정책이다.
//
// ChangedFieldsTables 에 든 표의 UPDATE 만 cdc_change_audit 에 남긴다.
// 비우면 끄고, "*"(또는 "all")가 들어 있으면 전체다. 나머지는 일부 목록이다.
// 전역 on/off 가 아니라 목록인 이유는 비용이다 — 이벤트당 INSERT 한 건이 더 붙어
// 대량 UPDATE 구간의 처리량이 그만큼 깎인다(Java 판 실측: 2,467 → 1,654 events/s).
// 감사가 필요한 표에만 켤 것.
type Audit struct {
	ChangedFieldsTables []string
}

// Target 은 적재 대상 DB 다.
type Target struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	MaxConns int32
}

// Apply 는 적용 실패를 다루는 정책이다.
type Apply struct {
	MaxBatchRetries       int
	RetryBackoff          time.Duration
	HaltOnDeadLetterRatio float64
}

// DeadLetter 는 격리 건 재처리 정책이다.
type DeadLetter struct {
	ReprocessEnabled  bool
	ReprocessInterval time.Duration
	ReprocessBatch    int
}

// Batch 는 스트림에서 받은 이벤트를 몇 건씩 묶어 한 트랜잭션으로 적용할지 정한다.
//
// go-pq-cdc 는 Debezium 과 달리 이벤트를 한 건씩 넘긴다. 한 건마다 트랜잭션을 열면
// 왕복이 이벤트 수만큼 늘고, 무엇보다 "적용과 진행 지점 기록이 한 트랜잭션"이라는
// 성질을 이벤트 단위로만 얻게 되어 처리량이 무너진다. 그래서 이 계층에서 다시 묶는다.
//
//	MaxSize   한 배치에 담을 최대 건수
//	MaxWait   이만큼 기다려도 배치가 안 차면 있는 만큼 적용한다.
//	          변경이 드문 구간에서 마지막 몇 건이 무한정 묶여 있지 않게 하는 상한이다
//	QueueSize 스트림과 적재 사이의 완충. 다 차면 스트림이 막히고, 그것이 곧 배압이다
type Batch struct {
	MaxSize   int
	MaxWait   time.Duration
	QueueSize int
}

// Load 는 환경변수를 읽어 설정을 만든다. 값이 없으면 기본값을 쓴다.
func Load() (Config, error) {
	cfg := Config{
		Source: Source{
			Name:              env("CDC_NAME", "embedded-cdc-go"),
			Host:              env("SOURCE_DB_HOST", "localhost"),
			Port:              envInt("SOURCE_DB_PORT", 57432),
			User:              env("SOURCE_DB_USER", "cdc_user"),
			Password:          env("SOURCE_DB_PASSWORD", "cdc_pass"),
			DBName:            env("SOURCE_DB_NAME", "sourcedb"),
			SlotName:          env("CDC_SLOT_NAME", "embedded_cdc_go_slot"),
			PublicationName:   env("CDC_PUBLICATION_NAME", "embedded_cdc_go_pub"),
			Tables:            envList("CDC_TABLES", []string{"car", "computer", "grade", "member"}),
			SnapshotEnabled:   envBool("CDC_SNAPSHOT_ENABLED", true),
			HeartbeatTable:    env("CDC_HEARTBEAT_TABLE", "cdc_heartbeat"),
			HeartbeatInterval: envDuration("CDC_HEARTBEAT_INTERVAL", 10*time.Second),
			FailOnCaptureGap:  envBool("CDC_FAIL_ON_CAPTURE_GAP", true),

			ClockSkewProbeInterval: envDuration("CDC_CLOCK_SKEW_PROBE_INTERVAL", 30*time.Second),
		},
		Target: Target{
			Host:     env("TARGET_DB_HOST", "localhost"),
			Port:     envInt("TARGET_DB_PORT", 57433),
			User:     env("TARGET_DB_USER", "postgres"),
			Password: env("TARGET_DB_PASSWORD", "postgres"),
			DBName:   env("TARGET_DB_NAME", "targetdb"),
			MaxConns: int32(envInt("TARGET_DB_MAX_CONNS", 8)),
		},
		Apply: Apply{
			MaxBatchRetries:       envInt("CDC_MAX_BATCH_RETRIES", 3),
			RetryBackoff:          envDuration("CDC_RETRY_BACKOFF", 200*time.Millisecond),
			HaltOnDeadLetterRatio: envFloat("CDC_HALT_DLQ_RATIO", 0.5),
		},
		DeadLetter: DeadLetter{
			ReprocessEnabled:  envBool("CDC_DLQ_REPROCESS", true),
			ReprocessInterval: envDuration("CDC_DLQ_INTERVAL", 30*time.Second),
			ReprocessBatch:    envInt("CDC_DLQ_BATCH_SIZE", 50),
		},
		Batch: Batch{
			MaxSize:   envInt("CDC_BATCH_MAX_SIZE", 500),
			MaxWait:   envDuration("CDC_BATCH_MAX_WAIT", 200*time.Millisecond),
			QueueSize: envInt("CDC_BATCH_QUEUE_SIZE", 8192),
		},
		Audit: Audit{
			// 비어 있으면 끔. compose 가 Java 판과 같은 기본값(car)을 준다.
			ChangedFieldsTables: envList("CDC_AUDIT_CHANGED_FIELDS", nil),
		},
		MetricPort: envInt("CDC_METRIC_PORT", 8080),
		LogLevel:   parseLevel(env("CDC_LOG_LEVEL", "info")),
	}
	return cfg, cfg.validate()
}

// TargetDSN 은 적재 대상 접속 문자열이다.
func (c Config) TargetDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.Target.User, c.Target.Password, c.Target.Host, c.Target.Port, c.Target.DBName)
}

// SourceDSN 은 원천 접속 문자열이다. 슬롯 상태 조회처럼 복제가 아닌 용도로 쓴다.
func (c Config) SourceDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.Source.User, c.Source.Password, c.Source.Host, c.Source.Port, c.Source.DBName)
}

func (c Config) validate() error {
	if len(c.Source.Tables) == 0 {
		return fmt.Errorf("캡처 대상 테이블이 비어 있다 (CDC_TABLES)")
	}
	if c.Apply.MaxBatchRetries < 0 {
		return fmt.Errorf("CDC_MAX_BATCH_RETRIES 는 0 이상이어야 한다")
	}
	if c.Apply.HaltOnDeadLetterRatio <= 0 || c.Apply.HaltOnDeadLetterRatio > 1 {
		return fmt.Errorf("CDC_HALT_DLQ_RATIO 는 0 초과 1 이하여야 한다")
	}
	if c.Batch.MaxSize <= 0 {
		return fmt.Errorf("CDC_BATCH_MAX_SIZE 는 1 이상이어야 한다")
	}
	return nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(env(key, ""), 64); err == nil {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v, err := strconv.ParseBool(env(key, "")); err == nil {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	// "200ms" 같은 Go 표기를 먼저 시도하고, 숫자만 오면 밀리초로 본다.
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	if ms, err := strconv.Atoi(raw); err == nil {
		return time.Duration(ms) * time.Millisecond
	}
	return fallback
}

func envList(key string, fallback []string) []string {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		// "public.car" 처럼 스키마가 붙어 와도 받아 준다 — Java 판 설정을 그대로 옮겨 오는 경우가 있다.
		p = strings.TrimSpace(p)
		if i := strings.LastIndex(p, "."); i >= 0 {
			p = p[i+1:]
		}
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

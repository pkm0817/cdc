// Package verification 은 캡처 신뢰성 검증 시나리오(V1~V6)다.
//
// 여기 있는 테스트는 단위 테스트가 아니다. 실제로 기동 중인 source/target
// PostgreSQL 에 붙어 슬롯을 만들고 끊고 되돌리며, 그때 무슨 일이 벌어지는지를 잰다.
// 통과/실패만이 아니라 "무엇이 얼마였는지"를 리포트로 남기는 것이 목적이다.
//
// 운영 테이블(car, computer, grade, member)과 embedded_cdc_go_slot 은 건드리지 않는다.
// verify_* 전용 테이블·publication·슬롯만 쓴다 — 기동 중인 cdc-service 와 겹치면
// 서로의 결과를 오염시키기 때문이다.
package verification

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	sourceOnce sync.Once
	targetOnce sync.Once
	sourcePool *pgxpool.Pool
	targetPool *pgxpool.Pool
)

// SourceURL 은 원천 접속 문자열이다. verify 스크립트가 환경변수로 준다.
func SourceURL() string {
	return env("CDC_VERIFY_SOURCE_URL",
		"postgres://postgres:postgres@localhost:57432/sourcedb?sslmode=disable")
}

// TargetURL 은 수신 측 접속 문자열이다.
func TargetURL() string {
	return env("CDC_VERIFY_TARGET_URL",
		"postgres://postgres:postgres@localhost:57433/targetdb?sslmode=disable")
}

// Source 는 원천 풀이다. 운영 코드의 풀을 쓰지 않는다 —
// 검증은 애플리케이션 조립과 무관하게 돌아야 엔진을 켜고 끄는 시나리오를 자유롭게 만들 수 있다.
func Source() *pgxpool.Pool {
	sourceOnce.Do(func() { sourcePool = mustOpen(SourceURL()) })
	return sourcePool
}

func Target() *pgxpool.Pool {
	targetOnce.Do(func() { targetPool = mustOpen(TargetURL()) })
	return targetPool
}

func mustOpen(url string) *pgxpool.Pool {
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		panic(fmt.Sprintf("DB 접속 실패: %s: %v", url, err))
	}
	if err := pool.Ping(context.Background()); err != nil {
		panic(fmt.Sprintf("DB 응답 없음: %s: %v — 스택이 떠 있는지 확인할 것", url, err))
	}
	return pool
}

// ── 접속 정보 조각 (하니스가 커넥터 설정을 만들 때 쓴다) ──────────────────────

type connInfo struct {
	host     string
	port     int
	user     string
	password string
	database string
}

func sourceConn() connInfo {
	cfg, err := pgx.ParseConfig(SourceURL())
	if err != nil {
		panic(fmt.Sprintf("source URL 을 읽지 못했다: %v", err))
	}
	return connInfo{
		host:     cfg.Host,
		port:     int(cfg.Port),
		user:     cfg.User,
		password: cfg.Password,
		database: cfg.Database,
	}
}

// ── 실행 도우미 ─────────────────────────────────────────────────────────────

// OnSource 는 문장 여러 개를 순서대로 실행한다. 하나라도 실패하면 panic 한다 —
// 검증 준비가 실패한 채로 진행하면 뒤따르는 계측치가 전부 의미를 잃는다.
func OnSource(statements ...string) {
	execAll(Source(), "source", statements...)
}

func OnTarget(statements ...string) {
	execAll(Target(), "target", statements...)
}

func execAll(pool *pgxpool.Pool, label string, statements ...string) {
	ctx := context.Background()
	for _, sql := range statements {
		if _, err := pool.Exec(ctx, sql); err != nil {
			panic(fmt.Sprintf("%s SQL 실행 실패: %s: %v", label, oneLine(sql), err))
		}
	}
}

// UpdateOnTarget 은 갱신 문장을 실행하고 실제로 영향받은 행 수를 돌려준다.
// 0 이면 조건절에 걸려 차단된 것이다.
func UpdateOnTarget(sql string, args ...any) int64 {
	tag, err := Target().Exec(context.Background(), sql, args...)
	if err != nil {
		panic(fmt.Sprintf("target 갱신 실패: %s: %v", oneLine(sql), err))
	}
	return tag.RowsAffected()
}

// ScalarOnSource 는 첫 열의 값 하나를 읽는다. dest 는 포인터여야 한다.
// 행이 없으면 false 를 돌려준다.
func ScalarOnSource(dest any, sql string, args ...any) bool {
	return scalar(Source(), dest, sql, args...)
}

func ScalarOnTarget(dest any, sql string, args ...any) bool {
	return scalar(Target(), dest, sql, args...)
}

func scalar(pool *pgxpool.Pool, dest any, sql string, args ...any) bool {
	err := pool.QueryRow(context.Background(), sql, args...).Scan(dest)
	if err == pgx.ErrNoRows {
		return false
	}
	if err != nil {
		panic(fmt.Sprintf("조회 실패: %s: %v", oneLine(sql), err))
	}
	return true
}

// CountOnSource 는 count(*) 처럼 항상 한 행이 나오는 조회의 값을 돌려준다.
func CountOnSource(sql string, args ...any) int64 {
	var n int64
	ScalarOnSource(&n, sql, args...)
	return n
}

func CountOnTarget(sql string, args ...any) int64 {
	var n int64
	ScalarOnTarget(&n, sql, args...)
	return n
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func oneLine(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// ctxBackground 는 검증 코드가 쓰는 기본 컨텍스트다.
// 시나리오마다 타임아웃을 다르게 두면 실패 원인이 "느려서"인지 "안 돼서"인지 흐려진다.
func ctxBackground() context.Context {
	return context.Background()
}

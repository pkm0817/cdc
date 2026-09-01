// Package persistence 는 target DB 어댑터다. ORM 없이 SQL 을 직접 쓴다.
//
// Java 판은 JPA + QueryDSL 이었지만 여기서는 pgx 로 문장을 그대로 쓴다.
// 이 파이프라인이 DB 에 하는 일은 "멱등 UPSERT 와 조건부 UPDATE" 뿐이고,
// 그 두 문장은 어차피 ON CONFLICT 라 어떤 ORM 으로도 표현되지 않는다.
// 엔티티 매핑 계층을 두면 얻는 것 없이 한 겹만 늘어난다.
package persistence

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// executor 는 pgx.Tx 와 *pgxpool.Pool 이 둘 다 만족하는 부분집합이다.
// 저장소 코드가 "지금 트랜잭션 안인가"를 신경 쓰지 않게 한다.
type executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type txKey struct{}

// Store 는 모든 저장소가 공유하는 접속 손잡이다.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// InTx 는 트랜잭션 경계를 연다. fn 이 오류를 돌려주면 롤백, nil 이면 커밋이다.
//
// fn 에 넘기는 ctx 에 트랜잭션을 실어 두므로, 그 안에서 부른 저장소 메서드는
// 전부 같은 트랜잭션을 탄다. Java 의 @Transactional 이 하던 일을 명시적으로 한 것이다.
//
// 이미 트랜잭션 안이면 새로 열지 않고 그대로 참여한다 (Spring 의 REQUIRED 와 같다).
// 중첩이 조용히 두 트랜잭션이 되면 "적용과 진행 지점 기록이 한 트랜잭션"이라는
// 이 파이프라인의 전제가 무너지므로, 규칙을 여기서 못 박아 둔다.
func (s *Store) InTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, already := ctx.Value(txKey{}).(pgx.Tx); already {
		return fn(ctx)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("트랜잭션 시작 실패: %w", err)
	}

	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		// 롤백 실패는 원래 오류를 가리지 않는다. 원인은 fn 이 돌려준 쪽이다.
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("커밋 실패: %w", err)
	}
	return nil
}

// exec 는 ctx 에 트랜잭션이 실려 있으면 그 안에서, 없으면 풀에서 바로 실행한다.
//
// DLQ 기록이 이 성질에 기대고 있다 — 실패한 적용의 롤백에 격리 기록이 휩쓸리면 안 되므로,
// DLQ 저장소는 트랜잭션이 실리지 않은 ctx 로 불린다 (Java 의 REQUIRES_NEW 자리).
func (s *Store) exec(ctx context.Context) executor {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return s.pool
}

// numeric 은 십진 문자열을 NUMERIC 파라미터로 바꾼다.
//
// 문자열을 그대로 넘기지 않는 이유는 파라미터 타입이 text 로 추론되어
// 값의 유효성 판정이 서버까지 미뤄지기 때문이다. 여기서 걸러야 DLQ 사유가 분명해진다.
func numeric(decimal string) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	if err := n.Scan(decimal); err != nil {
		return n, fmt.Errorf("NUMERIC 으로 바꿀 수 없다: %q: %w", decimal, err)
	}
	return n, nil
}

// lsnToInt64 는 LSN 을 BIGINT 컬럼에 넣을 수 있는 형태로 바꾼다.
//
// PostgreSQL 의 LSN 은 부호 없는 64비트지만 실제 값은 한참 아래에서 움직인다.
// 그래도 상한을 넘으면 음수로 접혀 순서 비교가 뒤집히므로, 조용히 넘기지 않고 막는다.
func lsnToInt64(lsn uint64) (int64, error) {
	if lsn >= uint64(1)<<62 {
		return 0, fmt.Errorf("LSN 이 BIGINT 로 담기에 너무 크다: %d", lsn)
	}
	return int64(lsn), nil
}

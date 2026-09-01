package port

import (
	"context"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
)

// CarRepository 는 target 의 car 저장소(outbound port)다.
//
// Upsert 는 반드시 멱등이어야 한다 — 재기동이나 재스냅샷으로 같은 행이 여러 번 도착한다.
type CarRepository interface {
	Upsert(ctx context.Context, car model.Car) error
	Delete(ctx context.Context, id int64) error
}

// ComputerRepository 는 target 의 computer 저장소(outbound port)다.
//
// car 와 달리 두 연산 모두 "더 새로운 이벤트일 때만" 반영해야 한다.
// 순서 역전 방어를 구현체가 아니라 이 계약에 못 박아 둔다 —
// 나중에 저장소를 갈아 끼워도 이 성질이 사라지지 않게 하기 위해서다.
type ComputerRepository interface {
	// UpsertIfNewer 는 저장된 source_lsn 보다 새로운 이벤트일 때만 반영한다.
	// 돌려주는 값은 실제로 반영된 행 수다. 0 이면 더 오래된 이벤트라 차단된 것이다.
	UpsertIfNewer(ctx context.Context, computer model.Computer) (int64, error)

	// SoftDelete 는 물리 삭제가 아니라 deleted 플래그를 세운다.
	// 물리 삭제하면 늦게 도착한 UPDATE 가 행을 되살려 유령 데이터가 남는다.
	SoftDelete(ctx context.Context, id int64, lsn uint64) (int64, error)
}

// GradeRepository 는 target 의 grade 저장소(outbound port)다.
//
// ComputerRepository 와 같은 계약이다. 다만 삭제가 소프트인 것은 여기서는 선택이 아니라 필수다.
// member 가 grade_id 로 이 행을 가리키고 있어서, 물리 삭제하면 남아 있는 member 의
// grade_id 가 어디도 가리키지 못하는 값이 된다. (target 에 FK 가 없으므로 DB 가 막아 주지도 않는다)
type GradeRepository interface {
	UpsertIfNewer(ctx context.Context, grade model.Grade) (int64, error)
	SoftDelete(ctx context.Context, id int64, lsn uint64) (int64, error)
}

// MemberRepository 는 target 의 member 저장소(outbound port)다.
//
// grade 를 참조하지만 이 계약에는 그 사실이 드러나지 않는다 —
// grade_id 는 검증 없이 그대로 적재된다. 부모가 아직 안 왔는지 확인하지 않는 이유는
// 확인해 봐야 할 수 있는 일이 "실패시키기"뿐이고, 그건 유실이 아니라 지연일 뿐인
// 상황을 DLQ 로 밀어 넣기 때문이다. 순서는 스트림이 보장하고, 어긋남은 대사가 잡는다.
type MemberRepository interface {
	UpsertIfNewer(ctx context.Context, member model.Member) (int64, error)
	SoftDelete(ctx context.Context, id int64, lsn uint64) (int64, error)
}

// CheckpointStore 는 파이프라인이 어디까지 처리했는지를 우리 손으로 기록하는 곳이다.
//
// go-pq-cdc 는 오프셋 파일을 쓰지 않는다. 진행 지점은 복제 슬롯의
// confirmed_flush_lsn 하나뿐이라, 슬롯이 사라지면 같이 사라진다.
// 그러면 "처음부터 다시 읽는 것"과 "구간을 건너뛴 것"을 구분할 수 없다.
// 수신 측 DB 에 남겨야 그 판단이 가능하다.
type CheckpointStore interface {
	// LastAppliedLSN 은 마지막으로 처리한 WAL 위치다. 두 번째 값이 false 면 최초 기동이다.
	LastAppliedLSN(ctx context.Context, pipeline string) (uint64, bool, error)

	// Record 는 배치 하나를 끝낼 때마다 부른다. 이벤트마다 부르면 왕복이 두 배가 된다.
	Record(ctx context.Context, pipeline string, lsn uint64) error
}

// DeadLetterStore 는 반영하지 못한 이벤트를 보관하는 곳(outbound port)이다.
//
// 여기 들어간 것은 유실이 아니라 "추적되는 미반영"이다.
// 원인을 고친 뒤 다시 처리할 수 있어야 하므로, 재구성에 필요한 값을 전부 남긴다.
type DeadLetterStore interface {
	// Store 는 역직렬화는 됐으나 적용에 실패한 경우다.
	Store(ctx context.Context, pipeline string, event model.ChangeEvent, cause error, attempts int) error

	// StoreUnparsable 은 역직렬화 자체가 안 된 경우다. 도메인 객체가 없으므로 원문을 그대로 남긴다.
	StoreUnparsable(ctx context.Context, pipeline, rawPayload string, cause error) error

	// PendingCount 는 아직 처리되지 않은 건수다. 경보의 기준이 된다.
	PendingCount(ctx context.Context, pipeline string) (int64, error)

	// ClaimForRetry 는 재처리 대상으로 표시된 건을 가져온다.
	//
	// PENDING 을 자동으로 집지 않는 이유가 있다. 원인이 고쳐졌는지는 사람만 안다.
	// 자동 재시도를 돌리면 고쳐지지 않은 독성 건이 영원히 재시도되며 잡음만 쌓인다.
	// 그래서 status 를 RETRY_REQUESTED 로 바꾸는 것이 명시적인 재처리 신청이 된다.
	ClaimForRetry(ctx context.Context, pipeline string, limit int) ([]model.PendingDeadLetter, error)

	MarkResolved(ctx context.Context, id int64) error

	// MarkRetryFailed 는 다시 PENDING 으로 돌리고 시도 횟수를 올린다.
	MarkRetryFailed(ctx context.Context, id int64, cause error) error
}

// FailureClassifier 는 적용 실패의 성격을 판정한다.
//
// 판정 근거(SQLSTATE 등)는 저장 기술에 딸린 지식이라 구현이 인프라에 있다.
// 응용 계층은 "재시도할지, 격리할지, 멈출지"만 알면 된다.
type FailureClassifier interface {
	Classify(cause error) model.FailureVerdict
}

// PipelineMetrics 는 파이프라인 관측 지표(outbound port)다.
//
// 구현체가 노출하는 이름은 Grafana 대시보드가 그대로 참조하므로 바꾸면 대시보드가 깨진다.
// Java 판과 이름을 일부러 맞춰 두었다 — 두 스택의 대시보드를 나란히 놓고 비교하기 위해서다.
//
//	cdc_events_total{table, op}
//	cdc_sink_errors_total{table}
//	cdc_dead_letters_total{table}
//	cdc_end_to_end_lag_seconds{table}
type PipelineMetrics interface {
	EventApplied(table, op string)
	ApplyFailed(table string)

	// DeadLettered 는 DLQ 로 격리된 건이다. 0 이 아니면 미반영 데이터가 쌓이고 있다는 뜻이다.
	DeadLettered(table string)

	// EndToEndLag 의 sourceCommittedAtMs 가 0 이하면 기록하지 않는다.
	EndToEndLag(table string, sourceCommittedAtMs int64)
}

// TransactionRunner 는 "한 트랜잭션 경계"를 여는 포트다.
//
// Java 판에서는 @Transactional 프록시가 이 일을 했다. Go 에는 그런 장치가 없으므로
// 경계를 명시적으로 연다. fn 이 오류를 돌려주면 롤백, nil 이면 커밋이다.
// fn 에 넘어오는 ctx 에는 트랜잭션이 실려 있어, 그 안에서 부른 저장소는 모두 같은 트랜잭션을 탄다.
type TransactionRunner interface {
	InTx(ctx context.Context, fn func(ctx context.Context) error) error
}

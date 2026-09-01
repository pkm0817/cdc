package model

// Operation 은 변경 이벤트의 종류다. 코드값은 Debezium 의 op 표기를 그대로 쓴다.
//
// go-pq-cdc 는 op 코드 대신 Go 타입(*format.Insert 등)으로 종류를 구분하지만,
// 여기서는 한 글자 코드로 되돌린다. 이유가 둘 있다.
//  1. DLQ 페이로드와 지표 라벨이 라이브러리 타입 이름에 묶이지 않는다
//  2. Java(Debezium) 판과 지표·대시보드를 그대로 나눠 쓸 수 있다
//
// SnapshotRead 와 Create 를 굳이 나눠 두지만 처리는 같다 —
// 둘 다 멱등 UPSERT 로 흘려보내야 스냅샷이 다시 돌아도 target 이 깨지지 않는다.
// 지표에서 "스냅샷이 얼마나 돌았는지"를 구분해 보기 위해 코드만 남겨 둔다.
type Operation string

const (
	SnapshotRead Operation = "r"
	Create       Operation = "c"
	Update       Operation = "u"
	Delete       Operation = "d"
)

// Code 는 지표 라벨과 DLQ 에 남길 한 글자 표기다.
func (o Operation) Code() string {
	return string(o)
}

// IsUpsert 는 DELETE 를 제외한 나머지가 전부 UPSERT 경로를 탄다는 뜻이다.
func (o Operation) IsUpsert() bool {
	return o != Delete
}

// OperationFromCode 는 이 파이프라인의 관심 밖인 코드에 false 를 돌려준다.
func OperationFromCode(code string) (Operation, bool) {
	switch Operation(code) {
	case SnapshotRead:
		return SnapshotRead, true
	case Create:
		return Create, true
	case Update:
		return Update, true
	case Delete:
		return Delete, true
	default:
		return "", false
	}
}

package model

// FailureVerdict 는 적용 실패를 어떻게 다룰지에 대한 판정이다.
//
// 분류가 둘(성공/실패)이 아니라 셋인 이유가 이 파이프라인 설계의 핵심이다.
// 모든 실패를 격리하면, target DB 를 재기동하는 30초 동안 전체 트래픽이 DLQ 로 쏟아진다.
// 그건 유실을 격리한 것이 아니라 옮겨 담은 것이다.
type FailureVerdict int

const (
	// VerdictRetry — 일시적이다. 시간이 지나면 저절로 성공한다: 커넥션 끊김, 데드락, 자원 부족.
	VerdictRetry FailureVerdict = iota

	// VerdictDeadLetter — 그 이벤트 하나의 데이터 문제다. 몇 번을 넣어도 똑같이 실패한다:
	// 제약 위반, 형식 오류.
	VerdictDeadLetter

	// VerdictHalt — 구조 문제다. 모든 이벤트가 실패한다: 테이블 없음, 권한 없음.
	// 격리하면 DLQ 가 전체를 삼킨다.
	VerdictHalt
)

func (v FailureVerdict) String() string {
	switch v {
	case VerdictRetry:
		return "RETRY"
	case VerdictDeadLetter:
		return "DEAD_LETTER"
	case VerdictHalt:
		return "HALT"
	default:
		return "UNKNOWN"
	}
}

// PendingDeadLetter 는 재처리를 기다리는 격리 건이다.
//
// DLQ 에 저장된 payload 로부터 원래 이벤트를 복원한 결과다.
// 복원이 가능하다는 것이 곧 "격리는 유실이 아니다"의 근거다.
type PendingDeadLetter struct {
	ID       int64
	Event    ChangeEvent
	Attempts int
}

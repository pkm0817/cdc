// Package application 은 배치 하나를 끝까지 책임지는 계층이다 — 재시도, 격리, 정지 판단.
package application

import "fmt"

// HaltError 는 파이프라인을 계속 돌리면 안 되는 상황이다.
//
// 이 오류가 밖으로 나가면 호출자(엔진 어댑터)가 ack 를 보내지 않고 프로세스를 내린다.
// 슬롯의 confirmed_flush_lsn 이 전진하지 않으므로 <b>멈춘 지점부터 다시 읽는다</b> —
// 유실 없이 사람의 개입을 기다리는 상태가 된다.
type HaltError struct {
	// Code 는 지표 라벨(cdc_pipeline_halts_total{reason})로 그대로 나간다.
	// 문장이 아니라 코드만 쓰는 이유는 카디널리티다 — 사유 문장을 라벨로 올리면
	// 메시지가 조금만 달라져도 시계열이 하나씩 늘어난다.
	Code   string
	Reason string
	Cause  error
}

// 정지 사유 코드. Java 판(ChangeEventService)과 같은 두 가지다.
//
//	HaltDLQRatio      한 배치에서 임계 이상이 격리됐다 — 개별 데이터 문제가 아니라 구조 문제
//	HaltUnrecoverable 계속 돌리면 안 되는 실패 (테이블 없음·권한 없음·기록 실패)
//	HaltShutdown      종료 중 취소. 사고가 아니므로 지표로 세지 않는다
const (
	HaltDLQRatio      = "DLQ_RATIO"
	HaltUnrecoverable = "UNRECOVERABLE"
	HaltShutdown      = "SHUTDOWN"
)

func (e *HaltError) Error() string {
	if e.Cause == nil {
		return "파이프라인 정지: " + e.Reason
	}
	return fmt.Sprintf("파이프라인 정지: %s (원인: %v)", e.Reason, e.Cause)
}

func (e *HaltError) Unwrap() error {
	return e.Cause
}

func halt(code, reason string, cause error) *HaltError {
	return &HaltError{Code: code, Reason: reason, Cause: cause}
}

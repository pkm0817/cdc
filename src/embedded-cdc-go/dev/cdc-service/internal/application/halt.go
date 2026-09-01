// Package application 은 배치 하나를 끝까지 책임지는 계층이다 — 재시도, 격리, 정지 판단.
package application

import "fmt"

// HaltError 는 파이프라인을 계속 돌리면 안 되는 상황이다.
//
// 이 오류가 밖으로 나가면 호출자(엔진 어댑터)가 ack 를 보내지 않고 프로세스를 내린다.
// 슬롯의 confirmed_flush_lsn 이 전진하지 않으므로 <b>멈춘 지점부터 다시 읽는다</b> —
// 유실 없이 사람의 개입을 기다리는 상태가 된다.
type HaltError struct {
	Reason string
	Cause  error
}

func (e *HaltError) Error() string {
	if e.Cause == nil {
		return "파이프라인 정지: " + e.Reason
	}
	return fmt.Sprintf("파이프라인 정지: %s (원인: %v)", e.Reason, e.Cause)
}

func (e *HaltError) Unwrap() error {
	return e.Cause
}

func halt(reason string, cause error) *HaltError {
	return &HaltError{Reason: reason, Cause: cause}
}

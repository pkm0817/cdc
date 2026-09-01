package model

// ChangeEvent 는 변경 한 건이다. 이 파이프라인이 다루는 유일한 입력이다.
//
//	Table      변경이 일어난 source 테이블명. 알 수 없는 이름도 그대로 담는다 —
//	           버릴지 말지는 도메인이 아니라 응용 계층이 판단하고 로그를 남긴다.
//	Op         변경의 종류
//	Before     변경 전 행. REPLICA IDENTITY FULL 이라 전체 컬럼이 담긴다. INSERT 는 nil
//	After      변경 후 행. DELETE 는 nil
//	LSN        WAL 상 위치. 단조 증가하는 정수라 순서 역전 방어의 기준 키로 쓴다
//	SourceTsMs source DB 가 이 변경을 보낸 시각(epoch millis). end-to-end 지연 계산용
type ChangeEvent struct {
	Table      string
	Op         Operation
	Before     *RowData
	After      *RowData
	LSN        uint64
	SourceTsMs int64
}

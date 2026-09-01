package model

import "time"

// Car 는 target 의 car 한 행이다. source 와 스키마가 같아 변환이 없다.
//
// ID 가 source 에서 발번된 값이라는 점만 다르다 — target 에는 시퀀스가 없다.
// Price 를 문자열로 들고 다니는 것은 손실 없이 옮기기 위해서다 (RowData.Decimal 주석 참고).
type Car struct {
	ID        int64
	Name      string
	Brand     string
	Price     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Computer 는 target 의 computer 한 행이다. source 와 컬럼이 전혀 다르다.
//
// Deleted 와 SyncedAt 은 여기 없다 —
// 전자는 삭제 경로에서만 세우고, 후자는 DB 가 now() 로 채우기 때문이다.
type Computer struct {
	ID        int64
	FullName  string
	Spec      string
	PriceKRW  string
	SourceLSN uint64
}

// Grade 는 target 의 grade 한 행이다. member 가 참조하는 부모 테이블이다.
//
// source 와 컬럼이 같지만 SourceLSN 이 붙는다 — computer 와 같은 이유로
// 순서 역전 방어가 필요하기 때문이다.
type Grade struct {
	ID           int64
	Code         string
	Name         string
	DiscountRate string
	CreatedAt    time.Time
	SourceLSN    uint64
}

// Member 는 target 의 member 한 행이다. grade 를 참조하는 자식 테이블이다.
//
// GradeID 는 그냥 int64 다 — Grade 참조가 아니다.
// 이벤트는 테이블마다 따로 오므로 member 이벤트를 받는 시점에 해당 grade 가
// target 에 있다는 보장이 없다. 객체 그래프를 만들려 들면 그 순간 조회가 필요해지고,
// 없으면 실패한다. CDC 는 행 단위 복제라 관계는 값(외래 키 컬럼)으로만 옮긴다.
type Member struct {
	ID        int64
	Email     string
	Name      string
	GradeID   int64
	Point     int32
	CreatedAt time.Time
	UpdatedAt time.Time
	SourceLSN uint64
}

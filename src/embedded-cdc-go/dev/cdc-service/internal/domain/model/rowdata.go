package model

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"
)

// ErrBadData 는 "이벤트에 실려 온 값 자체가 잘못됐다"는 뜻이다.
//
// 같은 값을 몇 번을 다시 넣어도 결과가 같으므로 재시도가 아니라 격리(DLQ) 대상이다.
// 실패 분류기가 이 센티널을 보고 DEAD_LETTER 로 판정한다 —
// 인프라 계층이 도메인 예외 타입을 하나하나 알 필요가 없게 하려는 경계다.
var ErrBadData = errors.New("이벤트 데이터 오류")

// RowData 는 이벤트에 실려 온 행 하나다. 컬럼명에서 값(문자열)으로 가는 map 이다.
//
// 도메인이 go-pq-cdc 의 타입을 모르게 하려고 두는 경계다.
// 값을 전부 문자열로 받아도 손실이 없다 — NUMERIC 은 pgtype.Numeric 에서
// 십진 문자열로 되돌릴 수 있고, 나머지 타입도 문자열에서 원래 타입으로 복원된다.
// 그리고 문자열이어야 DLQ 페이로드로 그대로 직렬화·복원된다.
//
// 값 타입이 *string 인 이유: "컬럼이 없다"와 "컬럼이 NULL 이다"를 구분해야 한다.
// TOAST 로 빠진 컬럼은 아예 없고, 진짜 NULL 은 nil 로 들어온다. 둘은 다른 사건이다.
type RowData struct {
	values map[string]*string
}

func NewRowData(values map[string]*string) RowData {
	copied := make(map[string]*string, len(values))
	for k, v := range values {
		copied[k] = v
	}
	return RowData{values: copied}
}

// Values 는 읽기용 복사본이다. 내부 map 을 그대로 내주면 도메인 밖에서 바뀔 수 있다.
func (r RowData) Values() map[string]*string {
	copied := make(map[string]*string, len(r.values))
	for k, v := range r.values {
		copied[k] = v
	}
	return copied
}

// Columns 는 실려 온 컬럼 이름을 정렬해 돌려준다. 오류 메시지와 대사에 쓴다.
func (r RowData) Columns() []string {
	names := make([]string, 0, len(r.values))
	for k := range r.values {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func (r RowData) Text(column string) (string, error) {
	return r.require(column)
}

func (r RowData) Int64(column string) (int64, error) {
	raw, err := r.require(column)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s 를 정수로 읽을 수 없다 (%q)", ErrBadData, column, raw)
	}
	return v, nil
}

func (r RowData) Int32(column string) (int32, error) {
	v, err := r.Int64(column)
	if err != nil {
		return 0, err
	}
	if v > 2147483647 || v < -2147483648 {
		return 0, fmt.Errorf("%w: %s 가 int 범위를 넘는다 (%d)", ErrBadData, column, v)
	}
	return int32(v), nil
}

// Decimal 은 NUMERIC 을 십진 문자열 그대로 돌려준다.
//
// float 으로 바꾸지 않는다는 것이 이 메서드의 내용이다 — 금액을 float 으로 옮기면
// 그 순간 원본과 다른 값이 되고, 그 사실은 한참 뒤 대사에서야 드러난다.
// 계산이 필요하면 DecimalRat 로 받아 정확 유리수로 다룬다.
func (r RowData) Decimal(column string) (string, error) {
	raw, err := r.require(column)
	if err != nil {
		return "", err
	}
	if _, ok := new(big.Rat).SetString(raw); !ok {
		return "", fmt.Errorf("%w: %s 를 십진수로 읽을 수 없다 (%q)", ErrBadData, column, raw)
	}
	return raw, nil
}

// DecimalRat 은 오차 없는 유리수로 돌려준다. 환율 곱셈 같은 계산에 쓴다.
func (r RowData) DecimalRat(column string) (*big.Rat, error) {
	raw, err := r.Decimal(column)
	if err != nil {
		return nil, err
	}
	rat, _ := new(big.Rat).SetString(raw)
	return rat, nil
}

// Timestamp 는 timestamptz 를 돌려준다. RowData 에는 RFC3339 문자열로 실려 온다.
func (r RowData) Timestamp(column string) (time.Time, error) {
	raw, err := r.require(column)
	if err != nil {
		return time.Time{}, err
	}
	t, parseErr := time.Parse(time.RFC3339Nano, raw)
	if parseErr != nil {
		return time.Time{}, fmt.Errorf("%w: %s 를 시각으로 읽을 수 없다 (%q)", ErrBadData, column, raw)
	}
	return t, nil
}

// require 는 컬럼이 없거나 NULL 이면 조용히 기본값을 주지 않고 오류를 낸다.
// 조용한 기본값은 잘못된 값을 target 에 적재하고, 그 사실이 한참 뒤에 발견된다.
func (r RowData) require(column string) (string, error) {
	v, present := r.values[column]
	if !present {
		return "", fmt.Errorf("%w: 이벤트에 컬럼이 없다: %s (수신한 컬럼: %v)",
			ErrBadData, column, r.Columns())
	}
	if v == nil {
		return "", fmt.Errorf("%w: 컬럼이 NULL 이다: %s", ErrBadData, column)
	}
	return *v, nil
}

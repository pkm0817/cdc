// Package port 는 도메인이 바깥과 주고받는 계약(포트)이다.
//
// Go 에서는 인터페이스를 "쓰는 쪽"에 두는 것이 관례지만, 여기서는 한곳에 모았다.
// 이 저장소의 목적이 육각형 구조를 눈으로 보여 주는 것이고, Java 판과 파일을
// 나란히 놓고 비교할 수 있어야 하기 때문이다. 실무 코드라면 소비 지점에 두는 편이 낫다.
//
// 트랜잭션 경계는 context 로 전달된다. 저장소 메서드는 ctx 만 받고, 그 ctx 에
// 트랜잭션이 실려 있으면 그 안에서, 없으면 풀에서 바로 실행된다.
// 시그니처에 트랜잭션 타입을 노출하면 도메인이 pgx 를 알게 되므로 그렇게 했다.
package port

import (
	"context"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
)

// ChangeEventHandler 는 파이프라인의 입구(inbound port)다. 변경 배치를 받아 target 에 반영한다.
//
// 한 건이 아니라 배치를 받는 이유는 두 가지다.
//  1. 적용과 진행 지점 기록을 한 트랜잭션으로 묶으려면 경계가 배치여야 한다
//  2. 이벤트마다 왕복하면 처리량이 왕복 횟수에 묶인다
//
// 이 메서드가 오류를 돌려주면 호출자는 슬롯에 ack 를 보내지 않는다.
// 즉 오류를 돌려주는 것이 곧 "유실 없이 멈춤"이다.
type ChangeEventHandler interface {
	Handle(ctx context.Context, pipeline string, batch []model.ChangeEvent) error
}

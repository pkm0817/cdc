# cdc-custom — WAL 을 읽지 않는 동기화

CDC 가 아니다. **두 DB 가 이미 같은 상태라는 전제** 위에서, "어느 행이 손대졌는지"만
기록해 두고 반영 시점에 소스의 현재 값을 읽어 맞춘다.

```
Source PostgreSQL ──트리거──▶ sync_outbox ──폴링──▶ sync-service ──JDBC 벌크──▶ Target PostgreSQL
   (업무 트랜잭션 안)          (PK + op 만)                        (접어서 한 번에)
```

## 왜 만들었나

기존 두 스택(embedded-cdc, embedded-cdc-go)을 대량 UPDATE 로 재보니 병목이 분명했다.
`REPLICA IDENTITY FULL` 때문에 UPDATE 1건이 변경 전·후 이미지를 함께 실어 **행당 약 1KB** 의
WAL 을 만들고, 그것을 전부 디코딩·전송·적용해야 했다. 30만 건 UPDATE 에 312MB 가 흘렀다.

여기서는 같은 30만 건이 outbox 에 **행당 약 30바이트** 로 적힌다. 게다가 같은 행이 100번
바뀌어도 반영은 한 번이다.

## 세 방식의 차이

| | embedded-cdc (Java) | embedded-cdc-go (Go) | **cdc-custom** |
|---|---|---|---|
| 변경 포착 | WAL 논리 디코딩 | WAL 논리 디코딩 | 트리거 → outbox 표 |
| 소스 설정 | `wal_level=logical`, 슬롯 | 〃 | **없음** |
| 옮기는 것 | 변경 전·후 전체 값 | 〃 | **PK + 연산** |
| 같은 행 반복 변경 | 횟수만큼 적용 | 〃 | **한 번으로 접힘** |
| 중간 상태 | 남는다 (감사 가능) | 남는다 | **남지 않는다** |
| 원천 위험 | 슬롯이 WAL 을 붙잡아 디스크 고갈 | 〃 | outbox 표 비대 |
| 소스 쓰기 비용 | 없음 (WAL 은 어차피 쓴다) | 없음 | **트리거 INSERT 가 붙는다** |

**중간 상태가 남지 않는다**는 점이 이 방식의 한계이자 정의다. "언제 무엇이 무엇으로
바뀌었나"를 물어야 하면 이 방식으로는 답할 수 없다. 최종 상태만 맞추면 되는 동기화에만 쓴다.

## 빠른 시작

docker 또는 podman 이 있으면 된다. 포트·컨테이너·네트워크가 58xxx 대역이라
기존 두 스택(56xxx, 57xxx)과 나란히 띄울 수 있다.

```bash
docker compose up -d --build     # DB 2개 + 모니터링 + 동기화 서비스
./scripts/up.sh                  # 같은 일을 하는 스크립트 (Windows 는 up.ps1)
```

| 접속 | 주소 | (참고) Java 판 / Go 판 |
|---|---|---|
| Grafana | http://localhost:58300 (admin/admin) | 56300 / 57300 |
| Prometheus | http://localhost:58290 | 56090 / 57090 |
| sync-service | http://localhost:58280/actuator/health | 56080 / 57080 |
| source DB | localhost:58432 (sourcedb) | 56432 / 57432 |
| target DB | localhost:58433 (targetdb) | 56433 / 57433 |

## 동작

1. **포착** — 업무 테이블 4개에 문장 단위(`FOR EACH STATEMENT`) 트리거가 걸려 있다.
   전이 테이블(`REFERENCING NEW TABLE`)로 바뀐 PK 를 `INSERT ... SELECT` 한 문장에 밀어 넣는다.
   행 단위 트리거였다면 30만 행 UPDATE 에서 30만 번 호출돼 부하 생성 쪽이 병목이 된다.
2. **접기** — 워커가 outbox 를 seq 순으로 읽으며 `(표, 행)` 기준으로 마지막 연산만 남긴다.
3. **반영** — 표별로 UPSERT 대상은 소스에서 `WHERE id = ANY(?)` 로 현재 값을 한 번에 읽어
   타깃에 벌크 UPSERT, 삭제 대상은 PK 만으로 처리한다.
4. **전진** — 체크포인트(`sync_checkpoint.last_seq`)를 반영과 **같은 트랜잭션**에서 옮긴다.
5. **비우기** — 반영이 끝난 outbox 구간을 지운다. 트랜잭션 밖이라 실패해도 무해하다.

### 순서 역전 문제가 없다

CDC 판에는 `source_lsn` 을 비교해 오래된 이벤트가 새 값을 덮어쓰지 않게 막는 가드가 있다.
여기서는 **항상 현재 값을 읽으므로** 그런 상황 자체가 생기지 않는다. 타깃의 `source_seq` 는
정합성 장치가 아니라 "이 행이 어느 시점까지 반영됐나"를 보기 위한 관측용이다.

### 재기동에 틈이 없다

Go 판은 체크포인트 기록보다 슬롯 ack 가 앞서 나가는 바람에, 평범한 재기동에서도 그 차이가
"되받을 수 없는 구간"으로 오인돼 기동이 막히는 사고가 있었다. 여기서는 반영과 체크포인트가
한 트랜잭션이라 그 상태가 존재할 수 없다. 컨테이너를 지웠다 만들어도 이어서 돈다
(오프셋 파일도 볼륨도 없다).

## 성능 손잡이

처리량은 스레드 수가 아니라 **배치 크기**로 올린다. 워커 스레드는 하나다 — 여러 스레드가
outbox 를 나눠 읽으면 같은 행이 다른 배치로 갈라져 접기 효과가 흩어진다.

```bash
SYNC_BATCH_SIZE=20000 docker compose up -d sync-service
```

## 지표

CDC 두 스택과 이름·라벨을 맞췄다. 같은 대시보드 쿼리로 셋을 비교할 수 있다.

| 지표 | 뜻 |
|---|---|
| `cdc_events_total{table,op}` | 타깃에 반영한 행 수 (세 스택 공통) |
| `cdc_end_to_end_lag_seconds{table}` | 변경 기록 → 타깃 반영까지 (세 스택 공통) |
| `sync_outbox_entries_total` | 접기 **전** 줄 수 |
| `sync_folded_rows_total` | 접은 **뒤** 반영한 행 수 |
| `sync_outbox_pending` | 아직 반영되지 않은 줄 수 (CDC 의 슬롯 지연에 해당) |
| `cdc_outbox_oldest_age_seconds` | 가장 오래 기다린 줄의 나이 |

**접기 비율**(`sync_outbox_entries_total / sync_folded_rows_total`)이 1 에 가까우면
이 방식을 쓸 이유가 없다는 뜻이다. 서로 다른 행만 한 번씩 바뀌는 부하에서는 접힐 것이
없어 CDC 대비 이점이 사라진다. 이 값을 먼저 보고 판단한다.

## 대시보드

CDC 두 스택과 **같은 세 장을 같은 자리에** 둔다. 어느 스택을 고르든 같은 질문을 같은 패널에서 보기 위해서다.

| 대시보드 | uid | 무엇을 보나 |
|---|---|---|
| CDC Custom (outbox) Overview | `cdc-custom` | 정합성·처리량·지연·outbox 적체·리소스 + DLQ·체크포인트·경보 |
| CDC PoC 검증 지표 (Custom/outbox) | `cdc-custom-poc` | PoC 질의 12개 축. 슬롯·WAL 자리는 이 방식의 등가물(outbox 적체·seq 전진·정체·트리거)로 |
| CDC 검증 리포트 (V1–V6, Custom/outbox) | `cdc-custom-verify` | V1~V6 를 이 방식에서 무엇이 되는지로 다시 읽은 것 |

행·패널 id·좌표가 CDC 두 스택과 같다. 쿼리만 다른데, 다른 이유는 두 가지뿐이다.

- **슬롯이 없다** — 보유 WAL → outbox 미반영 줄 수·표 크기, 슬롯 전진 → 체크포인트 seq 전진, heartbeat 나이 →
  "읽을 줄이 있는데 멈춘 시간", 캡처 갭 → 트리거 빠진 표 (`cdc_trigger_enabled`)
- **값을 싣지 않는다** — 필드 단위 변경 이력(`cdc_change_audit`)이 구조적으로 없다. 그 자리에는 DLQ 원문을 둔다.
  V5(TOAST) 시나리오도 이 방식에는 없다

경보 규칙은 `infra/monitoring/prometheus/rules/cdc-alerts.yml` 에 있다(13종). 대시보드 JSON 을 고치면
`node scripts/dashboard-outline.js --write infra/monitoring/grafana/dashboards/*.json` 으로 개요 파일을 같이 갱신한다.

## 같은 부하로 비교하기

시드·갱신 SQL 은 세 스택이 동일하다.

```bash
# 테이블당 10만 행 적재
podman exec -i cdc-custom-source-pg psql -U postgres -d sourcedb \
    -v ON_ERROR_STOP=1 -v cnt=100000 -f - < scripts/seed-bulk.sql

# 테이블당 10만 행 갱신
podman exec -i cdc-custom-source-pg psql -U postgres -d sourcedb \
    -v ON_ERROR_STOP=1 -v cnt=100000 -f - < scripts/update-bulk.sql
```

정합성은 세 스택 모두 같은 쿼리로 확인한다.

```sql
SELECT count(*), max(updated_at), sum(price) FROM car;   -- source 와 target 이 같아야 한다
```

## 알려진 제약

- **TRUNCATE 는 잡히지 않는다.** 전이 테이블을 지원하지 않아 트리거를 걸 수 없다.
  TRUNCATE 를 쓰면 타깃이 그대로 남으므로 전량 재적재로 맞춰야 한다(CDC 판이 TRUNCATE
  이벤트를 무시하는 것과 같은 처지다).
- **초기 적재는 별도다.** "두 DB 가 이미 같다"가 전제이므로, 처음 한 번은 덤프·복사 등으로
  맞춰 두고 시작해야 한다.
- **소스에 쓰기 비용이 붙는다.** 트리거 INSERT 가 업무 트랜잭션 안에서 돈다. CDC 는 원천에
  아무것도 더하지 않는다는 점에서, 이 방식이 무조건 유리한 것은 아니다.
- **중간 상태가 없다.** 위에 적은 대로 감사·이력에는 쓸 수 없다.

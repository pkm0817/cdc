# 아키텍처

`embedded-cdc` (Java/Debezium) 와 같은 설계를 Go + [go-pq-cdc](https://github.com/Trendyol/go-pq-cdc)
로 옮긴 것이다. 이 문서는 **왜 그렇게 되어 있는지**와 **옮기면서 달라진 것**만 적는다.
무엇이 어디 있는지는 README 의 폴더 구조를, 개별 판단 근거는 각 파일 주석을 볼 것.

## 1. 전체 흐름

```
                 ┌──────────────────────── source PostgreSQL ────────────────────────┐
                 │  car · computer · grade · member   (REPLICA IDENTITY FULL)        │
                 │  cdc_heartbeat                    (슬롯을 밀어 주는 살림살이)      │
                 │  publication: embedded_cdc_go_pub                                 │
                 │  slot:        embedded_cdc_go_slot                                │
                 └──────────────────────────────┬───────────────────────────────────┘
                                                │ logical replication (pgoutput, proto v2)
                                                ▼
   ┌───────────────────────────────── cdc-service (Go) ─────────────────────────────────┐
   │                                                                                    │
   │  infrastructure/cdc                                                                │
   │    Engine.listen   메시지 → Decode → 큐에 넣기만 한다 (적재는 하지 않는다)          │
   │    Engine.applyLoop 큐를 배치로 묶는다 (최대 500건 / 200ms)                        │
   │                     ── 여기가 Debezium 판과 갈리는 지점 ──                          │
   │    SlotContinuityGuard  기동 시 체크포인트 ↔ 슬롯 restart_lsn 대조                  │
   │    SQLStateClassifier   실패를 RETRY / DEAD_LETTER / HALT 로 판정                   │
   │                                                                                    │
   │  application                                                                       │
   │    ChangeEventService  배치 하나를 끝까지 책임진다 (재시도 → 격리 → 정지)           │
   │    BatchApplier        적용 + 체크포인트 기록 = 한 트랜잭션                          │
   │    DeadLetterReprocessor  RETRY_REQUESTED 로 표시된 건만 다시 반영                  │
   │                                                                                    │
   │  domain                                                                            │
   │    model / mapping / port  — pgx 도 go-pq-cdc 도 모른다                            │
   │                                                                                    │
   │  infrastructure/persistence  pgx + 손으로 쓴 SQL (ON CONFLICT ... WHERE)            │
   └────────────────────────────────────────┬───────────────────────────────────────────┘
                                            │ ack (배치 커밋 후에만)
                                            ▼
                        슬롯의 confirmed_flush_lsn 이 전진한다
```

## 2. 유실이 생길 수 없는 이유

### 2.1 ack 는 커밋 뒤에만

`Engine.flush` 의 순서가 전부다.

```
적용(ChangeEventService.Handle) → 커밋 → ack
```

뒤집으면 ack 한 구간이 적용되지 않은 채 슬롯만 전진해 조용한 유실이 된다.
적용이 실패하면 ack 를 아예 보내지 않으므로, 다음 기동에서 슬롯이 그 구간을 다시 흘려보낸다.
**ack 를 보내지 않는 것이 곧 "유실 없이 멈춤"이다.**

관심 밖 메시지(Relation, Commit, 스냅샷 알림 등)도 큐를 지나간다. 그 자리에서 바로 ack 하면
앞서 큐에 들어가 아직 적재되지 않은 이벤트를 건너뛴 위치까지 confirmed 가 전진해 버린다.

### 2.2 적용과 진행 지점이 같은 트랜잭션

`BatchApplier.ApplyAll` 이 배치 적용과 `cdc_checkpoint` 기록을 한 트랜잭션에 넣는다.
둘이 갈라져 있으면 "적용은 됐는데 어디까지 했는지는 모르는" 창이 생긴다.

Java 판에서는 `@Transactional` 프록시가 이 경계를 만들었지만 Go 에는 그 장치가 없다.
`persistence.Store.InTx` 가 트랜잭션을 context 에 실어 넘기고, 저장소는 ctx 에 실린 것이
있으면 그 안에서, 없으면 풀에서 바로 실행한다. DLQ 기록이 이 성질에 기대고 있다 —
격리 기록이 실패한 적용의 롤백에 휩쓸리면 안 되므로 트랜잭션이 실리지 않은 ctx 로 부른다.

### 2.3 중복은 막지 않고, 이중 반영을 막는다

논리 복제는 at-least-once 다. ack 전에 죽으면 그 구간이 다시 온다. 막을 대상은 중복 수신이
아니라 중복 반영이다. computer · grade · member 는 한 문장 안에서 순서를 판정한다.

```sql
ON CONFLICT (id) DO UPDATE SET ... WHERE EXCLUDED.source_lsn > <table>.source_lsn
```

판정이 한 문장 안에 있어야 한다. "조회해서 비교한 뒤 갱신"으로 벌어지면 그 사이에 다른
이벤트가 끼어든다. car 에는 이 가드가 없다 — 지금은 단일 고루틴 순차 처리가 그 전제를
지탱하고 있고, 적재를 병렬화하는 순간 이 표가 먼저 깨진다.

### 2.4 삭제는 소프트 삭제

물리 삭제하면 늦게 도착한 UPDATE 가 행을 되살려 유령 데이터가 남는다.
grade 는 여기에 더해 필수다 — member.grade_id 가 여전히 그 id 를 가리키고 있어서,
물리 삭제하면 남은 member 의 등급을 되짚을 수 없다.

## 3. 실패를 세 갈래로 나누는 이유

모든 실패를 격리하면 target DB 를 재기동하는 30초 동안 전체 트래픽이 DLQ 로 쏟아진다.
그건 유실을 격리한 것이 아니라 옮겨 담은 것이다.

| 판정 | 무엇 | 어떻게 |
|---|---|---|
| `RETRY` | 시간이 지나면 풀린다 (08/53/57 클래스, 40001, 40P01, 55P03) | 배치 전체 백오프 재시도 (200 → 400 → 800ms) |
| `DEAD_LETTER` | 그 건의 데이터 문제 (22/23 클래스, `model.ErrBadData`) | 건 단위로 좁혀 범인만 격리 |
| `HALT` | 구조 문제 (28/3D/3F/42 클래스) | 오류를 밖으로 내보내 멈춘다 |

판정하지 못한 오류는 `RETRY` 다. 한 번 더 시도해 보고 그래도 안 되면 건 단위 격리에서
어차피 DLQ 로 간다 — 모르는 것을 성급히 버리지 않기 위한 기본값이다.

한 배치에서 격리 비율이 `CDC_HALT_DLQ_RATIO`(기본 0.5)를 넘으면 개별 데이터 문제가 아니라
구조 문제로 보고 멈춘다. 그러지 않으면 수신 테이블이 통째로 사라졌을 때 DLQ 가 전체 트래픽을
삼킨다. 정지 판단이 DLQ 기록보다 **먼저**인 것도 같은 이유다 — 멈출 상황이면 애초에 개별
데이터 문제가 아니므로 격리해서는 안 된다.

## 4. Debezium 판에서 옮기며 달라진 것

### 4.1 진행 지점이 슬롯에만 있다 — 가드의 위상이 바뀐다

Debezium 은 오프셋 파일에 "여기서부터 읽어야 한다"를 적어 두고, 슬롯이 사라지면 그 지점을
더 이상 읽을 수 없다는 사실을 스스로 알아채 기동을 거부한다.

go-pq-cdc 에는 오프셋 파일이 없다. 진행 지점은 슬롯의 `confirmed_flush_lsn` 하나뿐이라,
슬롯이 사라지면 "어디까지 읽었는지"도 함께 사라진다. 라이브러리 입장에서는 최초 기동과
구분이 되지 않아 `slot.createIfNotExists` 설정대로 새 슬롯을 만들고 조용히 이어서 돈다.

그래서 `SlotContinuityGuard` 가 Java 판에서는 보조 수단이었지만 **여기서는 유일한 탐지
수단이다.** 판정 규칙은 같다.

1. 처리 이력(`cdc_checkpoint`)은 있는데 슬롯이 없다
2. 슬롯의 `restart_lsn` 이 마지막으로 처리한 지점보다 앞서 있다

둘 중 하나라도 걸리면 되받을 수 없는 구간이 생긴 것이고, `CDC_FAIL_ON_CAPTURE_GAP=true` 면
기동을 거부한다. 조용히 어긋난 채로 도는 것보다 멈추고 재동기화를 요구하는 편이 낫다.
V3 가 이 동작을 실측으로 확인한다.

### 4.2 이벤트가 한 건씩 온다 — 배치를 우리가 만든다

Debezium 의 `notifying(batch, committer)` 에 해당하는 API 가 없다. 한 건마다 트랜잭션을 열면
"적용과 진행 지점 기록이 한 트랜잭션"이라는 성질은 지켜지지만 왕복이 건수만큼 늘어난다.

그래서 리스너는 큐에 넣기만 하고(`Engine.listen`), 별도 고루틴이 크기/시간 기준으로 다시
묶는다(`Engine.applyLoop`). 큐가 가득 차면 리스너가 막히고, 그 막힘이 곧 배압이다 —
적재가 느리면 WAL 수신도 같이 느려지고 슬롯이 WAL 을 붙잡아 유실을 막는다.

### 4.3 TOAST 로 빠진 값의 모양이 다르다

Debezium 은 그 자리를 `__debezium_unavailable_value` 자리표시자로 채우지만,
go-pq-cdc 는 컬럼을 아예 싣지 않는다. `RowData` 에서 "키가 없는" 상태가 된다.
`RowData` 의 값 타입이 `*string` 인 이유가 여기 있다 — NULL 과 부재는 다른 사건이다.

그리고 REPLICA IDENTITY FULL 이면 라이브러리가 old 이미지의 값으로 그 자리를 메워 준다.
Debezium 은 before/after 를 그대로 주고 복원이 응용의 몫이었는데, 여기서는 after 만 봐도
현재 값을 알 수 있다. V5 가 두 설정(DEFAULT/FULL)의 차이를 실측한다.

### 4.4 heartbeat 이 간단해졌다

Debezium 에서는 `heartbeat.interval.ms` 만 켜면 슬롯이 전혀 전진하지 않았다 —
관심 테이블 집합 밖에서 만든 WAL 은 커넥터가 받지 못하기 때문이다. `heartbeat.action.query` 로
관심 테이블에 직접 WAL 을 만들어 줘야 했다.

go-pq-cdc 는 heartbeat 표를 publication 안에 두도록 강제하고(기동 시 검증한다) 그 표에
직접 쓴다. 표 하나만 지정하면 둘을 한꺼번에 해 주는 셈이다. V6 가 heartbeat 유무의 차이를
슬롯 전진량(bytes)으로 잰다.

### 4.5 원천 DB 에 쓰기 권한이 필요하다

heartbeat 표 갱신과 snapshot 메타데이터(`cdc_snapshot_job`, `cdc_snapshot_chunks`) 관리 때문이다.
후자는 여러 인스턴스가 스냅샷 청크를 나눠 잡을 수 있게 하려는 설계인데, 그 대가로 원천 DB 에
파이프라인 살림살이가 생긴다. 원천을 건드릴 수 없는 환경이라면 둘 다 꺼야 한다.

### 4.6 JPA/QueryDSL 이 사라졌다

이 파이프라인이 DB 에 하는 일은 "멱등 UPSERT 와 조건부 UPDATE" 뿐이고, 그 두 문장은
`ON CONFLICT ... WHERE` 라 어떤 ORM 으로도 표현되지 않는다. Java 판에서도 결국 네이티브
쿼리로 내려갔던 부분이다. 엔티티 매핑 계층을 두면 얻는 것 없이 한 겹만 늘어나므로
pgx + 손으로 쓴 SQL 로 갔다.

## 5. 관측

지표 이름을 Java 판과 일부러 똑같이 맞췄다. 대시보드 JSON 을 그대로 나눠 쓰고, 두 스택을
나란히 띄워 같은 눈금으로 비교하기 위해서다.

| 지표 | 뜻 |
|---|---|
| `cdc_events_total{table, op}` | 반영된 이벤트 수 (op: r=snapshot, c, u, d) |
| `cdc_sink_errors_total{table}` | 적재 실패 |
| `cdc_dead_letters_total{table}` | 격리된 건. 0 이 아니면 미반영이 쌓이고 있다 |
| `cdc_end_to_end_lag_seconds{_sum,_count,_max}` | source 송신 → target 반영 지연 |
| `cdc_source_rows` / `cdc_target_rows` | 양쪽 행 수 (정합성 판정의 근거) |
| `cdc_slot_lag_bytes` | 슬롯이 붙잡은 WAL. 서비스가 죽으면 계속 자란다 |
| `go_pq_cdc_*` | 라이브러리 자체 지표 (캡처 지연, 처리 지연, 슬롯 상태, 스냅샷 진행) |

우리 지표는 별도 서버를 띄우지 않고 `connector.SetMetricCollectors` 로 라이브러리 레지스트리에
얹는다. 스크랩 대상이 하나면 "어느 쪽이 안 긁혔지"를 따질 일이 없다.

**지연 지표의 기준 시각이 Java 판과 다르다.** Debezium 의 `source.ts_ms` 는 커밋 시각이지만
go-pq-cdc 가 주는 `MessageTime` 은 WAL 송신 시각이다. 커밋 직후에 보내므로 실무상 차이는
작지만, 두 스택의 지연 값을 같은 것으로 취급하면 안 된다. V1 이 이 차이와 시계 편차를 함께 남긴다.

## 6. 이 설계가 감수하는 것

- **car 에는 순서 가드가 없다.** 단일 고루틴 순차 처리가 전제다. 적재를 병렬화하거나
  인스턴스를 늘리면 이 표부터 깨진다.
- **환율이 상수다.** `mapping.UsdToKrw`. 실제 서비스라면 시점에 따라 달라지므로 별도 out
  포트로 나가야 한다.
- **DLQ 재처리에 car 는 안전하지 않다.** LSN 가드가 없어 재처리가 더 새로운 값을 덮어쓸 수 있다.
  car 를 재처리할 때는 그 사이 변경이 없었는지 확인해야 한다.
- **종료 중 남은 배치는 버린다.** 적용하려면 트랜잭션과 ack 가 필요한데 종료 중에 그 둘을
  안전하게 끝냈다고 보장할 수 없다. 적용하지 않았으므로 ack 도 나가지 않고 다음 기동에서 다시 읽는다.

# Embedded CDC (Go)

Kafka 없이 PostgreSQL → PostgreSQL 을 실시간 동기화하는 로그 기반 CDC 파이프라인.
[Trendyol/go-pq-cdc](https://github.com/Trendyol/go-pq-cdc) 를 Go 서비스에 내장했다.

```
Source PostgreSQL ──logical replication(pgoutput)──▶ cdc-service ──pgx──▶ Target PostgreSQL
     (WAL)                                       (go-pq-cdc)          (멱등 UPSERT)
```

`../embedded-cdc` (Spring Boot + Debezium Embedded) 와 **같은 DB 스키마·같은 모니터링 구성·같은 지표
이름**을 쓰는 자매 스택이다. 파이프라인 설계는 그대로 두고 구현 언어와 캡처 라이브러리만 바꿨을 때
무엇이 같고 무엇이 달라지는지를 나란히 놓고 보기 위한 것이다.

- **car** : source ↔ target 스키마 동일 → 1:1 복제
- **computer** : 스키마 다름 → 매핑(full_name/spec/price_krw) + 소프트 삭제 + LSN 순서 가드
- **grade / member** : 관계가 있는 테이블 쌍 (FK 는 source 에만)
- **모니터링** : Grafana(+Prometheus, postgres_exporter×2, podman-exporter, image-renderer) — 정합성·처리량·지연·slot lag·컨테이너 리소스, 대시보드 3종과 경보 23종

## 빠른 시작

docker 또는 podman 이 있으면 된다 (스크립트가 자동 감지).
Java 판과 **포트·컨테이너·네트워크·슬롯 이름이 전부 다르므로 나란히 띄울 수 있다.**

```powershell
./scripts/up.ps1     # 전체 기동 (루트 docker-compose.yml 하나로 DB 2개 + 모니터링 + 앱)
./scripts/demo.ps1   # INSERT/UPDATE/DELETE 를 흘리고 source/target 비교
./scripts/load.ps1   # 5분마다 테이블당 INSERT 1000 / UPDATE 500 / DELETE 100 (지속 부하)
./scripts/verify.ps1 # 캡처 신뢰성 검증 시나리오 V1~V6 (스택 기동 상태에서)
./scripts/down.ps1   # 정지 (-Wipe: 볼륨까지 삭제 → 다음 기동 시 snapshot 재실행)
```

macOS/Linux 는 `scripts/*.sh` 사용.

스크립트는 루트 `docker-compose.yml` 을 부를 뿐이라 아래와 같아도 된다.
네트워크·볼륨은 compose 가 만들고 지운다 — 미리 만들 필요 없다.

```bash
docker compose up -d --build   # 스택 루트에서
docker compose down -v
```

하위 compose 파일(`infra/db/*`, `infra/monitoring/*`, `dev/*`)은 루트가 `include` 로
끌어오는 조각이다. `-f` 로 하나씩 따로 올리면 파일마다 프로젝트가 갈려 같은
`container_name` 을 두고 충돌한다.

> **컨테이너 리소스 수집기는 엔진을 탄다.** 기본은 podman 소켓을 읽는
> `podman-exporter` 다. rootless podman 에는 `/var/lib/docker` 도 `docker.sock` 도 없고
> 컨테이너 cgroup 이 `user.slice` 아래로 들어가 cAdvisor 가 이름을 붙이지 못한다.
> docker 데몬 환경이면 `--profile cadvisor` 로 cAdvisor 를 대신 띄우고
> `infra/monitoring/prometheus/prometheus.yml` 의 cadvisor job 주석을 푼다.
> 대시보드는 `rules/container-resources.yml` 이 만드는 `cdc:container_*` 만 읽으므로 어느 쪽이든 같다.
>
> 소켓 uid 가 1000 이 아니면 `PODMAN_SOCK=/run/user/<uid>/podman/podman.sock` 로 덮어쓴다.

| 접속 | 주소 | (참고) Java 판 |
|---|---|---|
| Grafana | http://localhost:57300 (admin/admin) | 56300 |
| Prometheus | http://localhost:57090 | 56090 |
| cdc-service | http://localhost:57080/status · /metrics | 56080/actuator |
| source DB | localhost:57432 · sourcedb · postgres/postgres | 56432 |
| target DB | localhost:57433 · targetdb · postgres/postgres | 56433 |

## Debezium 판과 무엇이 다른가

같은 설계를 옮겼지만 캡처 라이브러리의 성질이 달라 **결과가 달라지는 지점이 다섯 곳** 있다.
전부 코드 주석과 V1~V6 검증 항목에 근거가 남아 있다.

| | Debezium Embedded (Java) | go-pq-cdc (Go) |
|---|---|---|
| 진행 지점 저장 | 오프셋 파일 + 슬롯 | **슬롯의 `confirmed_flush_lsn` 하나뿐** |
| 슬롯이 사라지면 | 기동을 거부한다 (스스로 탐지) | **조용히 새 슬롯을 만들고 이어서 돈다** (V3) |
| 이벤트 전달 단위 | 배치 (`RecordCommitter`) | 한 건씩 → **우리가 다시 배치로 묶는다** |
| TOAST 로 빠진 값 | `__debezium_unavailable_value` 자리표시자 | **컬럼 자체가 실리지 않는다.** 단 REPLICA IDENTITY FULL 이면 라이브러리가 old 이미지 값으로 메워 준다 (V5) |
| 유휴 구간 슬롯 전진 | `heartbeat.interval.ms` + `heartbeat.action.query` 를 **둘 다** 걸어야 한다 | heartbeat 표 하나만 지정하면 된다 (V6) |
| 원천 DB 에 남기는 것 | 없음 (읽기 전용 계정) | **snapshot 메타데이터 + heartbeat 표** (쓰기 권한 필요) |

가장 중요한 것은 두 번째 줄이다. Java 판에서 `SlotContinuityGuard` 는 "Debezium 이 못 잡는
경우(오프셋 파일까지 날아간 경우)"를 위한 보조 수단이었지만, **여기서는 유실을 탐지하는 유일한
수단이다.** 수신 측 `cdc_checkpoint` 표와 슬롯의 `restart_lsn` 을 기동 시 대조하는 그 코드가
없으면, 슬롯이 사라진 사고가 아무 흔적 없이 지나간다.

## 파이프라인이 유실을 다루는 방식

설계 자체는 Java 판과 같다. 요지는 세 가지다.

1. **ack 를 보내지 않는 것이 곧 "유실 없이 멈춤"이다.**
   적용 → 커밋 → ack 순서를 지킨다. 뒤집으면 ack 한 구간이 적용되지 않은 채 슬롯만 전진한다.
2. **실패를 세 갈래로 나눈다.** 모든 실패를 격리하면 target DB 를 재기동하는 30초 동안
   전체 트래픽이 DLQ 로 쏟아진다. 그건 유실을 격리한 것이 아니라 옮겨 담은 것이다.
   - `RETRY` 일시적 (커넥션·데드락) → 배치 전체 백오프 재시도
   - `DEAD_LETTER` 그 건의 데이터 문제 → 건 단위로 좁혀 범인만 격리
   - `HALT` 구조 문제 (테이블 없음·권한 없음) → 멈춘다
3. **격리된 것은 유실이 아니라 "추적되는 미반영"이다.** `cdc_dead_letter` 의 payload 로 원래
   이벤트를 복원할 수 있고, 사람이 원인을 고친 뒤 `status = 'RETRY_REQUESTED'` 로 표시한 건만
   재처리된다. 자동으로 집으면 고쳐지지 않은 독성 건이 영원히 재시도되며 잡음만 쌓인다.

```sql
-- DLQ 확인
SELECT id, source_table, op, failure_type, failure_message, attempts, status
  FROM cdc_dead_letter WHERE status = 'PENDING' ORDER BY id;

-- 원인을 고친 뒤 재처리 신청
UPDATE cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = 1;
```

## 검증 시나리오

캡처 신뢰성 시나리오 6종(V1~V6)이 `dev/cdc-service/test/verification` 에 구현돼 있다.
**스택이 떠 있는 상태에서** 실행한다 — 실제 source/target PostgreSQL 에 붙어 돌기 때문이다.
운영 테이블·`embedded_cdc_go_slot` 은 건드리지 않고 `verify_*` 전용 테이블·publication·슬롯만 쓴다.

```powershell
./scripts/verify.ps1            # 전체 (약 3분)
./scripts/verify.ps1 -Run V3    # 특정 시나리오만
```

| | 무엇을 확인하나 |
|---|---|
| V1 | INSERT/UPDATE/DELETE 지연, 변경 필드 식별, 1만 건 배치 처리량, LSN 단조 증가 |
| V2 | Provider 다운 구간의 변경분이 재기동 후 유실 없이 도착하는가, 그동안 WAL 이 얼마나 쌓이는가 |
| V3 | 슬롯이 사라지면 어떻게 되는가 — **라이브러리는 조용히 넘어가고, 가드가 잡는다** |
| V4 | ack 전에 중단되어 중복이 유입돼도 이중 반영되지 않는가 |
| V5 | TOAST 필드가 REPLICA IDENTITY 설정에 따라 어떻게 실려 오는가 |
| V6 | 건수·체크섬 대사가 유실을 검출하는가, heartbeat 로 슬롯이 전진하는가 |

산출물은 `dev/cdc-service/build/verification/results.md` (지연·처리량·WAL 보유량 등 계측치)다.
통과/실패와 별개로 "무엇이 얼마였는지"가 남는다.

## 대시보드

Java 판과 같은 세 장을 같은 구조로 둔다. 두 스택을 나란히 놓고 같은 눈금으로 보기 위해서다.

| 대시보드 | uid | 무엇을 보나 |
|---|---|---|
| Embedded CDC (Go) Overview | `embedded-cdc-go` | 정합성·처리량·지연·슬롯·리소스. 상시로 켜 두는 화면 |
| CDC PoC 검증 지표 (Go) | `cdc-go-poc` | PoC 질의 12개 축(유실·silent failure·failover·offset·중단·중복·lag·retention·스키마·모니터링·운영·후속 계측) |
| CDC 검증 리포트 (V1–V6, Go) | `embedded-cdc-go-verify` | V1~V6 시나리오별 통과 기준·현재 측정치·남은 결정 |

Java 판과 **지표 이름·표 모양을 맞춰 두었기 때문에** 패널 쿼리가 그대로 쓰인다. 다른 곳은 둘뿐이다.

- **런타임 지표** — JVM 힙·GC 대신 `go_memstats_*` · `go_gc_duration_seconds` · `go_goroutines`
- **스냅샷 진행률** — 이쪽에만 있다. go-pq-cdc 가 진행 상황을 원천 `cdc_snapshot_job` 에 남기기 때문이다

변경 이력(`cdc_change_audit`)도 Java 판과 같은 표·같은 지표로 남긴다. 한 가지 차이는 TOAST 로 빠진 컬럼이
자리표시자 대신 **키 부재**로 온다는 것이고, 그 컬럼은 `unreadable_fields` 로 분리해 기록한다.

경보 규칙은 `infra/monitoring/prometheus/rules/cdc-alerts.yml` 에 있고, 상태는 Prometheus **Alerts**
탭과 PoC 대시보드의 "현재 발생 중인 경보" 패널에서 본다. Java 판과 판정이 갈리는 곳은
그 파일 상단 주석에 적어 두었다 — 요지는 **캡처 갭을 LSN 뺄셈으로 판정하지 않는다**는 것이다.
heartbeat 가 슬롯만 밀고 체크포인트는 밀지 않아 유휴 구간에 정상적으로 음수가 되기 때문이다.

### 대시보드 변경 추적

JSON 은 3,000줄이 넘어 패널에 쿼리 한 줄을 더해도 `git diff` 에서 묻힌다. 그래서 "무엇을
그리는가"만 뽑은 개요 파일(`*.outline.txt`)을 JSON 옆에 같이 둔다. 리뷰는 이 파일의 diff 로 한다.

```bash
node scripts/dashboard-outline.js --write infra/monitoring/grafana/dashboards/*.json  # 커밋 전 필수
node scripts/dashboard-outline.js --check infra/monitoring/grafana/dashboards/*.json  # 뒤처지면 exit 1
```

## 폴더 구조

```
embedded-cdc-go/
├── infra/
│   ├── db/            # source/target PostgreSQL compose (분리) + init SQL
│   └── monitoring/    # Prometheus + Grafana + postgres_exporter, 대시보드 프로비저닝
├── dev/
│   ├── cdc-service/   # Go 서비스 (멀티스테이지 Dockerfile)
│   │   ├── cmd/cdc-service/       # 조립만 하는 main
│   │   ├── internal/
│   │   │   ├── domain/            # model · mapping · port  (바깥을 모른다)
│   │   │   ├── application/       # 배치 트랜잭션 · 재시도/격리/정지 · DLQ 재처리
│   │   │   └── infrastructure/    # cdc(go-pq-cdc) · persistence(pgx) · metrics · config
│   │   └── test/verification/     # V1~V6 검증 시나리오
│   └── docker-compose.app.yml
├── scripts/           # up / demo / load / verify / down (ps1 + sh) + dashboard-outline.js
└── docs/architecture.md
```

육각형 구조를 그대로 옮겼다. `domain` 은 pgx 도 go-pq-cdc 도 모르고, 라이브러리 타입이 등장하는
곳은 `infrastructure/cdc/decoder.go` 하나뿐이다. Go 관례로는 인터페이스를 쓰는 쪽에 두지만
여기서는 `domain/port` 에 모았다 — Java 판과 파일을 나란히 놓고 비교할 수 있어야 하기 때문이다.

## 로컬 개발 (컨테이너 없이 앱만)

```powershell
# DB·모니터링은 컨테이너로 띄운 상태에서
cd dev/cdc-service
go run ./cmd/cdc-service    # localhost:57432/57433 으로 붙는다 (config 기본값)
```

의존성은 `vendor/` 에 함께 커밋돼 있다. 사내망처럼 `proxy.golang.org` 로 나가지 못하는
환경에서도 컨테이너 이미지가 빌드되어야 하기 때문이다 (Dockerfile 이 `-mod=vendor` 로 빌드한다).
모듈 프록시를 쓸 수 있는 환경이라면 `vendor/` 를 지우고 Dockerfile 의 `-mod=vendor` 만 빼면 된다.

설정은 전부 환경변수다 (`internal/infrastructure/config/config.go`). 자주 쓰는 것만 추리면:

| 변수 | 기본값 | 뜻 |
|---|---|---|
| `CDC_SNAPSHOT_ENABLED` | `true` | 최초 기동 시 전량 초기 적재. 원천에 쓰기 권한이 없으면 끈다 |
| `CDC_FAIL_ON_CAPTURE_GAP` | `true` | 되받을 수 없는 구간을 발견하면 기동을 거부 |
| `CDC_BATCH_MAX_SIZE` / `CDC_BATCH_MAX_WAIT` | `500` / `200ms` | 몇 건씩 묶어 한 트랜잭션으로 적용할지 |
| `CDC_HALT_DLQ_RATIO` | `0.5` | 한 배치에서 이 비율 넘게 격리되면 구조 문제로 보고 멈춤 |
| `CDC_HEARTBEAT_TABLE` | `cdc_heartbeat` | 비우면 heartbeat 끔 (유휴 구간에 WAL 이 쌓인다) |
| `CDC_DLQ_REPROCESS` | `true` | DLQ 재처리 루프 |
| `CDC_CLOCK_SKEW_PROBE_INTERVAL` | `30s` | 원천 DB 시계와의 편차 측정 주기. 0 이면 끔 (지연 수치의 신뢰도를 판정할 수 없게 된다) |
| `CDC_AUDIT_CHANGED_FIELDS` | `car` (compose) | UPDATE 의 변경 필드를 `cdc_change_audit` 에 남길 표. 비우면 끔 · `car,computer` 일부 · `*` 전체. Java 판과 같은 표·같은 지표(`cdc_change_audit_rows_total`) |

## 원천 DB 에 쓰기 권한이 필요한 이유

Java 판의 `cdc_user` 는 `REPLICATION` + `SELECT` 만 가졌지만, 여기서는 두 가지가 더 필요하다.

- **heartbeat** — `cdc_heartbeat` 표에 직접 INSERT/UPDATE 한다
- **snapshot** — go-pq-cdc 는 스냅샷 진행 상황을 원천 DB 의 `cdc_snapshot_job`,
  `cdc_snapshot_chunks` 에 남긴다 (여러 인스턴스가 청크를 나눠 잡을 수 있게 하려는 설계다)

원천을 건드릴 수 없는 환경이라면 `CDC_SNAPSHOT_ENABLED=false`, `CDC_HEARTBEAT_TABLE=` 로 끄고
초기 적재를 따로 해야 한다. 그 대가는 유휴 구간의 WAL 누적(V6 참고)이다.

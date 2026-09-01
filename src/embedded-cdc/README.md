# Embedded CDC

Kafka 없이 PostgreSQL → PostgreSQL 을 실시간 동기화하는 로그 기반 CDC 파이프라인.
Debezium Embedded Engine 을 Spring Boot(Java 21) 서비스에 내장했다.

```
Source PostgreSQL ──logical replication(pgoutput)──▶ cdc-service ──JDBC──▶ Target PostgreSQL
     (WAL)                                    (Debezium Embedded)        (멱등 UPSERT)
```

- **car** : source ↔ target 스키마 동일 → 1:1 복제
- **computer** : 스키마 다름 → 매핑(full_name/spec/price_krw) + 소프트 삭제 + LSN 순서 가드
- **모니터링** : Grafana(+Prometheus, postgres_exporter×2) — 정합성·처리량·지연·slot lag

전체 설명은 **[docs/architecture.html](docs/architecture.html)** 참고 (브라우저로 열면 된다).

## 빠른 시작

docker 또는 podman 이 있으면 된다 (스크립트가 자동 감지).

```powershell
./scripts/up.ps1     # 전체 기동: 네트워크 → DB 2개 → 모니터링 → 앱 빌드+기동
./scripts/demo.ps1   # INSERT/UPDATE/DELETE 를 흘리고 source/target 비교
./scripts/load.ps1   # 5분마다 테이블당 INSERT 1000 / UPDATE 500 / DELETE 100 (지속 부하)
./scripts/verify.ps1 # 캡처 신뢰성 검증 테스트 V1~V6 (스택 기동 상태에서)
./scripts/down.ps1   # 정지 (-Wipe: 볼륨까지 삭제 → 다음 기동 시 snapshot 재실행)
```

macOS/Linux 는 `scripts/*.sh` 사용.

| 접속 | 주소 |
|---|---|
| Grafana | http://localhost:56300 (admin/admin) |
| Prometheus | http://localhost:56090 |
| cdc-service | http://localhost:56080/actuator/health |
| source DB | localhost:56432 · sourcedb · postgres/postgres |
| target DB | localhost:56433 · targetdb · postgres/postgres |

## 검증 테스트

캡처 신뢰성 시나리오 6종(V1~V6)이 `dev/cdc-service/src/test` 에 구현돼 있다.
**스택이 떠 있는 상태에서** 실행한다 — 실제 source/target PostgreSQL 에 붙어 돌기 때문이다.
운영 테이블·`embedded_cdc_slot` 은 건드리지 않고 `verify_*` 전용 테이블·publication·슬롯만 쓴다.

```powershell
./scripts/verify.ps1              # 전체 (약 3분)
./scripts/verify.ps1 -Tests "*V3*"  # 특정 시나리오만
```

| | 무엇을 확인하나 |
|---|---|
| V1 | INSERT/UPDATE/DELETE 지연, 변경 필드 식별, 1만 건 배치 처리량 |
| V2 | Provider 다운 구간의 변경분이 재기동 후 유실 없이 도착하는가 |
| V3 | 슬롯이 사라지면 조용히 넘어가지 않고 기동을 거부하는가, 재동기화로 복구되는가 |
| V4 | 오프셋 flush 전 중단으로 중복이 유입돼도 이중 반영되지 않는가 |
| V5 | TOAST 필드가 REPLICA IDENTITY 설정에 따라 어떻게 실려 오는가 |
| V6 | 건수·체크섬 대사가 유실을 검출하는가, heartbeat 로 슬롯이 전진하는가 |

산출물은 두 개다.

- `dev/cdc-service/build/verification/results.md` — 지연·처리량·WAL 보유량 등 계측치
- `dev/cdc-service/build/reports/tests/test/index.html` — JUnit 리포트

`JAVA_HOME` 이 없으면 스크립트가 `~/.jdks` 에서 JDK 21 을 찾아 쓴다.

## 폴더 구조

```
embedded-cdc/
├── infra/
│   ├── db/            # source/target PostgreSQL compose (분리) + init SQL
│   └── monitoring/    # Prometheus + Grafana + postgres_exporter, 대시보드 프로비저닝
├── dev/
│   ├── cdc-service/   # Spring Boot + Debezium Embedded (Gradle, 멀티스테이지 Dockerfile)
│   └── docker-compose.app.yml
├── scripts/           # up / demo / load / verify / down (ps1 + sh)
└── docs/architecture.html
```

## 로컬 개발 (컨테이너 없이 앱만)

```powershell
# DB·모니터링은 컨테이너로 띄운 상태에서
cd dev/cdc-service
./gradlew bootRun    # localhost:56432/56433 으로 붙는다 (application.yml 기본값)
```

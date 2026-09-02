# Embedded CDC

Kafka 없이 PostgreSQL → PostgreSQL 을 실시간 동기화하는 로그 기반 CDC 파이프라인.
Debezium Embedded Engine 을 Spring Boot(Java 21) 서비스에 내장했다.

```
Source PostgreSQL ──logical replication(pgoutput)──▶ cdc-service ──JDBC──▶ Target PostgreSQL
     (WAL)                                    (Debezium Embedded)        (멱등 UPSERT)
```

- **car** : source ↔ target 스키마 동일 → 1:1 복제
- **computer** : 스키마 다름 → 매핑(full_name/spec/price_krw) + 소프트 삭제 + LSN 순서 가드
- **모니터링** : Grafana(+Prometheus, postgres_exporter×2, podman-exporter) — 정합성·처리량·지연·slot lag·컨테이너 리소스

전체 설명은 **[docs/architecture.html](docs/architecture.html)** 참고 (브라우저로 열면 된다).

## 빠른 시작

docker 또는 podman 이 있으면 된다 (스크립트가 자동 감지).

```powershell
./scripts/up.ps1     # 전체 기동 (루트 docker-compose.yml 하나로 DB 2개 + 모니터링 + 앱)
./scripts/demo.ps1   # INSERT/UPDATE/DELETE 를 흘리고 source/target 비교
./scripts/load.ps1   # 5분마다 테이블당 INSERT 1000 / UPDATE 500 / DELETE 100 (지속 부하)
./scripts/verify.ps1 # 캡처 신뢰성 검증 테스트 V1~V6 (스택 기동 상태에서)
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
> 대시보드는 `rules/container-resources.yml` 이 만드는 `cdc:container_*` 만 읽으므로
> 어느 쪽이든 같다.
>
> 소켓 uid 가 1000 이 아니면 `PODMAN_SOCK=/run/user/<uid>/podman/podman.sock` 로 덮어쓴다.

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

## 대시보드 변경 추적

Grafana 대시보드는 JSON 파일이 원본이고, provisioning 이 10초마다 다시 읽는다.
문제는 이 JSON 이 3,000줄이 넘어 **패널에 쿼리 한 줄을 더해도 `git diff` 에서 묻힌다**는 것이다.
좌표·색·폰트가 값보다 자리를 많이 차지하기 때문이다.

그래서 "무엇을 그리는가"만 뽑은 개요 파일(`*.outline.txt`)을 JSON 옆에 같이 둔다.
리뷰는 이 파일의 diff 로 한다.

```bash
# JSON 을 고친 뒤 개요를 다시 만든다 (커밋 전 필수)
node scripts/dashboard-outline.js --write infra/monitoring/grafana/dashboards/*.json

# 개요가 JSON 보다 뒤처지지 않았는지 확인 (뒤처지면 exit 1)
node scripts/dashboard-outline.js --check infra/monitoring/grafana/dashboards/*.json

# 두 판을 직접 견주기 — 화면(라이브)과 파일이 갈라졌는지 볼 때도 쓴다
node scripts/dashboard-outline.js --diff 이전.json 이후.json
```

개요에는 패널 제목·타입·위치, 쿼리(refId·expr·legend), 단위·소수점, 임계값, 설명만 담는다.
즉 **바뀌면 화면이 달라지는 것**만 남기고 나머지는 버린다.

> `provider.yml` 이 `allowUiUpdates: true` 라 Grafana 화면에서도 편집·저장이 된다.
> 그렇게 저장한 내용은 Grafana DB 에만 남고 파일에는 반영되지 않으며, 파일이 다시 바뀌면 덮어써진다.
> **화면에서 고쳤으면 JSON 으로 내보내 파일에 반영할 것.** 갈라졌는지는 이렇게 확인한다.
>
> ```bash
> curl -s -u admin:admin http://localhost:56300/api/dashboards/uid/cdc-poc \
>   | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(JSON.stringify(JSON.parse(s).dashboard)))" > live.json
> node scripts/dashboard-outline.js --diff live.json infra/monitoring/grafana/dashboards/embedded-cdc-poc.json
> ```

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

# PostgreSQL → MySQL CDC 동기화

PostgreSQL `members` 테이블의 변경을 Debezium 으로 캡처해, **스키마가 다른** MySQL `user`
테이블에 Spring Boot 애플리케이션이 반영한다.

```
PostgreSQL(members)  →  Debezium PG Connector  →  Kafka  →  Spring Boot  →  MySQL(user)
   WAL / logical           (Kafka Connect)      pg.public.members   매핑+UPSERT
```

설계 배경과 판단 근거는 `D:\brain\domain\wiki\concepts\CDC로 RDB 간 동기화하기.md`,
`PostgreSQL CDC 구축.md` 참고.

## 사전 요구사항

**Docker 만 있으면 된다.** Spring Boot 도 컨테이너에서 빌드하므로 로컬 JDK/Gradle 은 필요 없다.

> ⚠️ **WSL 안의 docker 를 쓰는 경우 (Docker Desktop 미설치)**
> WSL 세션이 하나도 남지 않으면 배포판이 종료되면서 **컨테이너가 전부 SIGTERM 으로 죽는다**
> (`Exited (143)`). 백그라운드에서 살려둘 것:
> ```powershell
> wsl -e sh -lc "sleep infinity"    # 별도 창에서 실행해 두고 작업
> ```
> WSL 안에서 직접 작업할 때는 `.sh` 스크립트를 쓴다:
> ```sh
> cd /mnt/d/src/cdc
> docker compose up -d --build
> sh scripts/register-connector.sh
> sh scripts/demo.sh
> ```

## 포트

호스트 포트는 다른 스택과 충돌하지 않도록 일부를 옮겨 두었다. **컨테이너 간 통신은 원래
포트를 쓰므로 커넥터 설정에는 영향이 없다.**

| 서비스 | 호스트 | 컨테이너 |
|---|---|---|
| PostgreSQL | **55432** | 5432 |
| MySQL | 3306 | 3306 |
| Kafka | 9092 | 9092 |
| Kafka Connect | **8084** | 8083 |

## 실행

```powershell
docker compose up -d --build      # 5개 컨테이너 기동 (최초 빌드 3~5분)
.\scripts\register-connector.ps1  # Debezium 커넥터 등록 + 상태 확인
.\scripts\demo.ps1                # UPDATE / INSERT / DELETE 후 MySQL 확인
```

`register-connector.ps1` 이 `connector: RUNNING`, `task[0]: RUNNING` 을 출력하면 정상이다.

`demo.sh` / `demo.ps1` 를 돌리면 네 가지 `op` 가 전부 흐르는 것을 확인할 수 있다:

```
synced op=r lsn=26643152 e2e=998ms     ← snapshot 이 읽은 기존 2건
synced op=u lsn=26666016 e2e=554ms     ← UPDATE
synced op=c lsn=26666712 e2e=225ms     ← INSERT
synced op=d lsn=26667032 e2e=347ms     ← DELETE (소프트 삭제로 반영)
```

동기화 로그 확인:

```powershell
docker compose logs -f member-sync
```

`synced op=r lsn=... e2e=...ms` 형태로 찍힌다. `e2e` 는 `Sink write 시각 − source.ts_ms` 로,
CDC 파이프라인의 end-to-end 지연이다.

## 정리

```powershell
docker compose down -v
```

> ⚠️ **`-v` 를 빼지 말 것.** PostgreSQL 의 replication slot 은 만료되지 않으므로, 커넥터가
> 멈춘 채 슬롯이 남아 있으면 WAL 이 삭제되지 않고 무한정 쌓여 **원천 DB 디스크가 찬다.**
> MySQL 은 반대로 binlog 가 만료되어 사라지는 것이 문제인데, PostgreSQL 은 안 사라지는 것이
> 문제다. 볼륨째 지우면 슬롯도 함께 사라진다.
>
> 컨테이너를 살려둔 채 커넥터만 내릴 때는 슬롯을 직접 지운다:
> ```sql
> SELECT pg_drop_replication_slot('member_cdc_slot');
> ```

## 스키마 매핑

애플리케이션 컨슈머를 직접 만든 **유일한 이유**가 이것이다. 컬럼명이 같다면 JDBC Sink
Connector 설정만으로 코드 없이 끝난다.

| PostgreSQL `members` | MySQL `user` |
|---|---|
| `member_id` | `id` |
| `full_name` | `name` |
| `email_address` | `email` |
| `status` | `user_status` |
| — | `deleted` (소프트 삭제) |
| — | `source_lsn` (결정적 시퀀싱) |

## 구조

```
docker-compose.yml              postgres / mysql / kafka / connect / member-sync
init/postgres/01-init.sql       members 테이블, REPLICA IDENTITY FULL, cdc_user, publication
init/mysql/01-init.sql          user 테이블 (+ deleted, source_lsn)
connector/                      Debezium 커넥터 설정
scripts/                        커넥터 등록 · 데모
member-sync/                    Spring Boot (헥사고날: adapter.in → application → adapter.out)
```

## 설계상 짚어둔 것

| 항목 | 처리 |
|---|---|
| **멱등성** | at-least-once 가 기본이므로 `r`/`c`/`u` 를 전부 UPSERT 로 |
| **순서 역전** | `source.lsn` 이 더 낮으면 UPDATE 를 무시 (UPSERT 안에서 `IF` 로) |
| **삭제** | 물리 삭제 대신 `deleted=1`. 물리 삭제하면 늦게 온 UPDATE 가 행을 되살린다 |
| **`before` 완전성** | `REPLICA IDENTITY FULL` — 없으면 DELETE 이벤트의 `before` 가 PK 만 담긴다 |
| **converter** | `schemas.enable=false` — 켜두면 payload 보다 스키마 블록이 수십 배 |
| **역직렬화 실패** | `ErrorHandlingDeserializer` 로 감싸 컨슈머 전체가 멈추지 않게 |

> ⚠️ **이 파이프라인은 SCD 유형 1(덮어쓰기)이라 이력이 남지 않는다.** 회원정보 변경 이력이
> 나중에라도 필요하면 지금 유형 2(`_START_AT`/`_END_AT` 버전 행 추가)로 바꿔야 한다 —
> "일단 최신화만 하고 이력은 나중에" 는 성립하지 않는다. 그 사이 변경분은 이미 사라진 뒤다.

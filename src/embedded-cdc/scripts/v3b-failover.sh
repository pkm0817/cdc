#!/usr/bin/env bash
# ============================================================================
# V3-b. DB 재기동과 이중화 전환에서 복제 슬롯이 어떻게 되는가
#
#   ./scripts/v3b-failover.sh
#
# V3 본편(v3-capture-gap-report.html)은 슬롯을 강제로 삭제해 "슬롯이 없는 상태"를
# 만들고 그 뒤를 측정했다. 시나리오 문서가 요구한 "DB 재기동 / 이중화 전환에서도
# 같은 상황이 생기는지"는 PG16 문서 근거로 갈음하고 실측하지 않았다.
# 이 스크립트가 그 빈칸을 채운다.
#
# 재는 것은 여섯 가지다.
#   A-1  정상 재기동(SIGTERM)          -> 슬롯이 남는가
#   A-2  강제 종료(SIGKILL) 후 크래시 복구 -> 슬롯이 남는가
#   A-3  데이터 볼륨 유실 후 기동        -> 슬롯이 남는가
#   B-1  standby 로 스트리밍 중          -> 슬롯이 standby 에 생기는가
#   B-2  pg_promote() 로 페일오버        -> 승격된 노드에 슬롯이 있는가
#   B-3  새 primary 에 붙는 소비자        -> 가드가 캡처 갭으로 보는가
#
# 운영/PoC 스택을 건드리지 않는다. 프로젝트 볼륨(embedded-cdc_*)과 무관한
# 일회용 컨테이너 두 개와 볼륨 하나만 쓰고 끝나면 지운다.
# 기동 플래그는 infra/db/docker-compose.source.yml 과 같게 맞췄다 —
# 조건이 다르면 결과를 이 스택의 근거로 쓸 수 없다.
# ============================================================================
set -euo pipefail

PRIMARY=v3b-pg
STANDBY=v3b-standby
NET=v3bnet
VOL=v3b-standby-data
SLOT=v3b_slot
IMAGE=postgres:16

engine=podman
command -v podman >/dev/null 2>&1 || engine=docker

# Git Bash 가 컨테이너 안 경로(/var/lib/...)를 윈도우 경로로 바꾸는 것을 막는다
export MSYS_NO_PATHCONV=1

PGFLAGS=(-c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 -c wal_sender_timeout=5min)

cleanup() {
  $engine rm -f "$PRIMARY" "$STANDBY" >/dev/null 2>&1 || true
  $engine volume rm "$VOL"            >/dev/null 2>&1 || true
  $engine network rm "$NET"           >/dev/null 2>&1 || true
}
trap cleanup EXIT

psql_p() { $engine exec "$PRIMARY" psql -U postgres -d sourcedb "$@"; }
psql_s() { $engine exec "$STANDBY" psql -U postgres -d sourcedb "$@"; }

await() {  # await <container>
  for _ in $(seq 1 60); do
    $engine exec "$1" pg_isready -U postgres -d sourcedb >/dev/null 2>&1 && return 0
  done
  echo "$1 이 뜨지 않았다" >&2; exit 1
}

slots() { # slots <container>
  $engine exec "$1" psql -U postgres -d sourcedb -Atc \
    "SELECT coalesce(string_agg(slot_name||' '||slot_type||' '||restart_lsn||' '||wal_status, ', '), '(없음)')
       FROM pg_replication_slots"
}

start_primary() {
  $engine run -d --name "$PRIMARY" --network "$NET" \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=sourcedb \
    "$IMAGE" postgres "${PGFLAGS[@]}" >/dev/null
  await "$PRIMARY"
  # pg_basebackup 이 붙을 수 있게 열어 준다 (일회용 컨테이너라 trust 로 둔다)
  $engine exec "$PRIMARY" sh -c \
    "echo 'host replication all all trust' >> /var/lib/postgresql/data/pg_hba.conf"
  psql_p -Atc "SELECT pg_reload_conf()" >/dev/null
}

seed() {
  # 슬롯 생성은 쓰기를 한 트랜잭션 안에서 못 한다 — 문장을 나눠 보낸다
  psql_p -Atc "CREATE TABLE car (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
               ALTER TABLE car REPLICA IDENTITY FULL;
               CREATE PUBLICATION v3b_pub FOR TABLE car;" >/dev/null
  psql_p -Atc "SELECT pg_create_logical_replication_slot('$SLOT','pgoutput')" >/dev/null
  psql_p -Atc "INSERT INTO car(name) SELECT 'car-'||g FROM generate_series(1,100) g" >/dev/null
}

echo "== 준비 =================================================================="
cleanup
$engine network create "$NET" >/dev/null
start_primary
seed

echo "PostgreSQL : $(psql_p -Atc 'SELECT version()' | cut -d, -f1)"
echo "sync_replication_slots 파라미터 존재 여부 : $(psql_p -Atc "SELECT count(*) FROM pg_settings WHERE name='sync_replication_slots'")  (PG17 부터 1)"
echo "기준 슬롯   : $(slots "$PRIMARY")"

echo
echo "== A. DB 재기동 =========================================================="

$engine restart "$PRIMARY" >/dev/null; await "$PRIMARY"
echo "A-1 정상 재기동(SIGTERM)      : $(slots "$PRIMARY")"

$engine kill -s KILL "$PRIMARY" >/dev/null; sleep 1
$engine start "$PRIMARY" >/dev/null; await "$PRIMARY"
echo "A-2 강제 종료 후 크래시 복구  : $(slots "$PRIMARY")"
$engine logs "$PRIMARY" 2>&1 | grep -c "not properly shut down" >/dev/null \
  && echo "    (크래시 복구가 실제로 돌았다: 'database system was not properly shut down')"

$engine rm -f "$PRIMARY" >/dev/null; start_primary
echo "A-3 데이터 볼륨 유실 후 기동  : $(slots "$PRIMARY")"
seed   # 이후 단계를 위해 다시 만든다

echo
echo "== B. 이중화 전환 ========================================================"

$engine volume create "$VOL" >/dev/null
$engine run --rm --network "$NET" -v "$VOL":/var/lib/postgresql/data --user postgres "$IMAGE" \
  pg_basebackup -h "$PRIMARY" -p 5432 -U postgres -D /var/lib/postgresql/data -R -X stream >/dev/null 2>&1
$engine run -d --name "$STANDBY" --network "$NET" -v "$VOL":/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=postgres "$IMAGE" postgres "${PGFLAGS[@]}" >/dev/null
await "$STANDBY"

echo "B-1 standby 스트리밍 중"
echo "    primary 슬롯 : $(slots "$PRIMARY")"
echo "    standby 슬롯 : $(slots "$STANDBY")"
echo "    복제 자체는 됐는가 — car 행 primary $(psql_p -Atc 'SELECT count(*) FROM car') / standby $(psql_s -Atc 'SELECT count(*) FROM car'), standby publication $(psql_s -Atc 'SELECT count(*) FROM pg_publication')개"

psql_s -Atc "SELECT pg_promote(wait => true, wait_seconds => 30)" >/dev/null
sleep 2
psql_s -Atc "CHECKPOINT" >/dev/null
echo "B-2 페일오버(pg_promote) 후"
echo "    in_recovery  : $(psql_s -Atc 'SELECT pg_is_in_recovery()')  (f = 승격됨)"
echo "    타임라인     : $(psql_s -Atc 'SELECT timeline_id FROM pg_control_checkpoint()')  (1 -> 2 면 전환된 것)"
echo "    승격 노드 슬롯: $(slots "$STANDBY")"

echo "B-3 새 primary 에 붙는 CDC 소비자 — SlotContinuityGuard 와 같은 판정"
echo "    처리 이력은 있는데 슬롯이 없는가(= 캡처 갭) : $(psql_s -Atc "SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT')")"

echo
echo "== 정리 (컨테이너·볼륨·네트워크 삭제) ===================================="

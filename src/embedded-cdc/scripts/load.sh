#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# 부하 발생기 — source DB 의 car / computer 에 주기적으로 변경을 일으킨다.
#
#   한 사이클(기본 5분 간격)마다 테이블당
#     INSERT 1000행 → UPDATE 500행 → DELETE 100행
#   순서로 실행한다. INSERT 를 먼저 해야 UPDATE/DELETE 가 때릴 행이 있다.
#
#   INSERT / UPDATE / DELETE 는 각각 별개 트랜잭션으로 커밋된다(psql 자동커밋).
#   한 덩어리로 묶으면 WAL 에 커밋 하나로만 찍혀 CDC 지연 관측이 무의미해진다.
#
# 사용법:
#   ./scripts/load.sh                 # 5분 간격 무한 반복 (Ctrl+C 로 중단)
#   ./scripts/load.sh -n 3            # 3사이클만 돌고 종료
#   ./scripts/load.sh -i 60           # 1분 간격
#   ./scripts/load.sh -t car          # car 만
#   INSERTS=200 ./scripts/load.sh     # 건수 조정 (INSERTS/UPDATES/DELETES)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

INTERVAL="${INTERVAL:-300}"     # 사이클 간격(초). 5분
CYCLES="${CYCLES:-0}"           # 0 = 무한
INSERTS="${INSERTS:-1000}"
UPDATES="${UPDATES:-500}"
DELETES="${DELETES:-100}"
TABLES="${TABLES:-car computer}"

while getopts "i:n:t:h" opt; do
    case "$opt" in
        i) INTERVAL="$OPTARG" ;;
        n) CYCLES="$OPTARG" ;;
        t) TABLES="$OPTARG" ;;
        h) sed -n '2,22p' "$0"; exit 0 ;;
        *) exit 2 ;;
    esac
done

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

SRC_CONTAINER="${SRC_CONTAINER:-emb-cdc-source-pg}"
TGT_CONTAINER="${TGT_CONTAINER:-emb-cdc-target-pg}"

# psql 이 각 문장마다 찍는 "INSERT 0 1000" 태그를 그대로 살려 진행 상황을 본다.
src() { $engine exec -i "$SRC_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d sourcedb "$@"; }
tgt() { $engine exec -i "$TGT_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d targetdb "$@"; }

scalar() { "$1" -tAc "$2" | tr -d '[:space:]'; }

$engine inspect "$SRC_CONTAINER" >/dev/null 2>&1 || {
    echo "source 컨테이너($SRC_CONTAINER)가 없다. 먼저 scripts/up.sh 로 스택을 띄운다." >&2
    exit 1
}

# ── car ─────────────────────────────────────────────────────────────────────
# UPDATE/DELETE 대상 선정에 ORDER BY random() 을 쓰지 않는다. 행이 수십만 건
# 쌓이면 매 사이클 풀스캔+정렬이 되어 부하 발생기 자신이 병목이 된다.
# 대신 id 범위 안에서 임의의 시작점을 잡고 PK 인덱스를 따라 N행만 훑는다.
car_sql() {
cat <<SQL
INSERT INTO car (name, brand, price)
SELECT 'car-' || to_char(now(), 'YYYYMMDDHH24MISS') || '-' || g,
       (ARRAY['Hyundai','Kia','Tesla','BMW','Benz','Toyota','Volvo'])[1 + (random() * 6)::int],
       (10000000 + random() * 60000000)::numeric(12,2)
  FROM generate_series(1, $INSERTS) AS g;

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM car),
     pick AS (
       SELECT c.id FROM car c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $UPDATES, 1))::bigint
        ORDER BY c.id LIMIT $UPDATES
     )
UPDATE car SET price      = (price * (0.90 + random() * 0.20))::numeric(12,2),
               updated_at = now()
 WHERE id IN (SELECT id FROM pick);

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM car),
     pick AS (
       SELECT c.id FROM car c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $DELETES, 1))::bigint
        ORDER BY c.id LIMIT $DELETES
     )
DELETE FROM car WHERE id IN (SELECT id FROM pick);
SQL
}

# ── computer ────────────────────────────────────────────────────────────────
# computer 에는 updated_at 이 없다(source 스키마 그대로). 값만 흔든다.
computer_sql() {
cat <<SQL
INSERT INTO computer (brand, model, cpu, ram_gb, price_usd)
SELECT (ARRAY['Apple','Lenovo','Dell','ASUS','HP','Samsung'])[1 + (random() * 5)::int],
       'model-' || to_char(now(), 'YYYYMMDDHH24MISS') || '-' || g,
       (ARRAY['M4 Pro','M4 Max','Core Ultra 7','Core Ultra 9','Ryzen 9','Ryzen 7'])[1 + (random() * 5)::int],
       (ARRAY[8,16,24,32,48,64,128])[1 + (random() * 6)::int],
       (600 + random() * 4000)::numeric(10,2)
  FROM generate_series(1, $INSERTS) AS g;

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM computer),
     pick AS (
       SELECT c.id FROM computer c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $UPDATES, 1))::bigint
        ORDER BY c.id LIMIT $UPDATES
     )
UPDATE computer SET price_usd = (price_usd * (0.90 + random() * 0.20))::numeric(10,2),
                    ram_gb    = (ARRAY[8,16,24,32,48,64,128])[1 + (random() * 6)::int]
 WHERE id IN (SELECT id FROM pick);

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM computer),
     pick AS (
       SELECT c.id FROM computer c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $DELETES, 1))::bigint
        ORDER BY c.id LIMIT $DELETES
     )
DELETE FROM computer WHERE id IN (SELECT id FROM pick);
SQL
}

trap 'echo; echo "중단됨 (완료 사이클: $((cycle - 1)))"; exit 0' INT TERM

echo "── 부하 발생기 시작 ───────────────────────────────"
echo "  대상      : $TABLES"
echo "  사이클    : INSERT $INSERTS / UPDATE $UPDATES / DELETE $DELETES  (테이블당)"
echo "  간격      : ${INTERVAL}초"
echo "  반복      : $([ "$CYCLES" -eq 0 ] && echo '무한 (Ctrl+C 로 중단)' || echo "${CYCLES}회")"
echo

cycle=1
while [ "$CYCLES" -eq 0 ] || [ "$cycle" -le "$CYCLES" ]; do
    started=$(date +%s)
    echo "── cycle #$cycle  $(date '+%Y-%m-%d %H:%M:%S') ───────────────────"

    for t in $TABLES; do
        printf '  %-9s ' "$t"
        # 태그 3줄(INSERT/UPDATE/DELETE)을 한 줄로 접어 찍는다.
        "${t}_sql" | src -f - | tr '\n' ' '
        echo
    done

    elapsed=$(( $(date +%s) - started ))

    src_car=$(scalar src "SELECT count(*) FROM car")
    src_com=$(scalar src "SELECT count(*) FROM computer")
    tgt_car=$(scalar tgt "SELECT count(*) FROM car")
    tgt_com=$(scalar tgt "SELECT count(*) FROM computer WHERE deleted = false")

    echo "  소요 ${elapsed}s | source car=$src_car computer=$src_com | target(직후) car=$tgt_car computer=$tgt_com"

    cycle=$((cycle + 1))
    if [ "$CYCLES" -ne 0 ] && [ "$cycle" -gt "$CYCLES" ]; then break; fi

    # 작업 시간을 뺀 만큼만 잔다 — 사이클이 5분 경계에서 밀리지 않게 한다.
    remain=$(( INTERVAL - elapsed ))
    [ "$remain" -gt 0 ] || remain=1
    echo "  다음 사이클까지 ${remain}s 대기"
    echo
    sleep "$remain"
done

echo
echo "완료 — $((cycle - 1)) 사이클"

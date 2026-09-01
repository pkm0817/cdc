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
#   .\scripts\load.ps1                    # 5분 간격 무한 반복 (Ctrl+C 로 중단)
#   .\scripts\load.ps1 -Cycles 3          # 3사이클만 돌고 종료
#   .\scripts\load.ps1 -IntervalSec 60    # 1분 간격
#   .\scripts\load.ps1 -Tables car        # car 만
#   .\scripts\load.ps1 -Inserts 200 -Updates 100 -Deletes 20
# ─────────────────────────────────────────────────────────────────────────────
param(
    [int]$IntervalSec = 300,   # 사이클 간격(초). 5분
    [int]$Cycles      = 0,     # 0 = 무한
    [int]$Inserts     = 1000,
    [int]$Updates     = 500,
    [int]$Deletes     = 100,
    [string[]]$Tables = @("car", "computer")
)

$ErrorActionPreference = "Stop"

$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }
$srcContainer = "emb-cdc-go-source-pg"
$tgtContainer = "emb-cdc-go-target-pg"

& $engine inspect $srcContainer *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Error "source 컨테이너($srcContainer)가 없다. 먼저 .\scripts\up.ps1 로 스택을 띄운다."
}

# 여러 문장이 담긴 스크립트를 stdin 으로 밀어 넣는다(-f -).
function Invoke-SourceScript([string]$sql) {
    $sql | & $engine exec -i $srcContainer psql -v ON_ERROR_STOP=1 -U postgres -d sourcedb -f -
}
function Get-SourceScalar([string]$sql) {
    (& $engine exec -i $srcContainer psql -tA -U postgres -d sourcedb -c $sql) -join "" -replace '\s', ''
}
function Get-TargetScalar([string]$sql) {
    (& $engine exec -i $tgtContainer psql -tA -U postgres -d targetdb -c $sql) -join "" -replace '\s', ''
}

# ── car ─────────────────────────────────────────────────────────────────────
# UPDATE/DELETE 대상 선정에 ORDER BY random() 을 쓰지 않는다. 행이 수십만 건
# 쌓이면 매 사이클 풀스캔+정렬이 되어 부하 발생기 자신이 병목이 된다.
# 대신 id 범위 안에서 임의의 시작점을 잡고 PK 인덱스를 따라 N행만 훑는다.
$carSql = @"
INSERT INTO car (name, brand, price)
SELECT 'car-' || to_char(now(), 'YYYYMMDDHH24MISS') || '-' || g,
       (ARRAY['Hyundai','Kia','Tesla','BMW','Benz','Toyota','Volvo'])[1 + (random() * 6)::int],
       (10000000 + random() * 60000000)::numeric(12,2)
  FROM generate_series(1, $Inserts) AS g;

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM car),
     pick AS (
       SELECT c.id FROM car c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $Updates, 1))::bigint
        ORDER BY c.id LIMIT $Updates
     )
UPDATE car SET price      = (price * (0.90 + random() * 0.20))::numeric(12,2),
               updated_at = now()
 WHERE id IN (SELECT id FROM pick);

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM car),
     pick AS (
       SELECT c.id FROM car c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $Deletes, 1))::bigint
        ORDER BY c.id LIMIT $Deletes
     )
DELETE FROM car WHERE id IN (SELECT id FROM pick);
"@

# ── computer ────────────────────────────────────────────────────────────────
# computer 에는 updated_at 이 없다(source 스키마 그대로). 값만 흔든다.
$computerSql = @"
INSERT INTO computer (brand, model, cpu, ram_gb, price_usd)
SELECT (ARRAY['Apple','Lenovo','Dell','ASUS','HP','Samsung'])[1 + (random() * 5)::int],
       'model-' || to_char(now(), 'YYYYMMDDHH24MISS') || '-' || g,
       (ARRAY['M4 Pro','M4 Max','Core Ultra 7','Core Ultra 9','Ryzen 9','Ryzen 7'])[1 + (random() * 5)::int],
       (ARRAY[8,16,24,32,48,64,128])[1 + (random() * 6)::int],
       (600 + random() * 4000)::numeric(10,2)
  FROM generate_series(1, $Inserts) AS g;

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM computer),
     pick AS (
       SELECT c.id FROM computer c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $Updates, 1))::bigint
        ORDER BY c.id LIMIT $Updates
     )
UPDATE computer SET price_usd = (price_usd * (0.90 + random() * 0.20))::numeric(10,2),
                    ram_gb    = (ARRAY[8,16,24,32,48,64,128])[1 + (random() * 6)::int]
 WHERE id IN (SELECT id FROM pick);

WITH b AS (SELECT min(id) AS lo, max(id) AS hi FROM computer),
     pick AS (
       SELECT c.id FROM computer c, b
        WHERE c.id >= b.lo + floor(random() * greatest(b.hi - b.lo - $Deletes, 1))::bigint
        ORDER BY c.id LIMIT $Deletes
     )
DELETE FROM computer WHERE id IN (SELECT id FROM pick);
"@

$scripts = @{ car = $carSql; computer = $computerSql }

Write-Host "── 부하 발생기 시작 ───────────────────────────────"
Write-Host "  대상      : $($Tables -join ' ')"
Write-Host "  사이클    : INSERT $Inserts / UPDATE $Updates / DELETE $Deletes  (테이블당)"
Write-Host "  간격      : ${IntervalSec}초"
Write-Host "  반복      : $(if ($Cycles -eq 0) { '무한 (Ctrl+C 로 중단)' } else { "${Cycles}회" })"
Write-Host ""

$cycle = 1
while ($Cycles -eq 0 -or $cycle -le $Cycles) {
    $started = Get-Date
    Write-Host "── cycle #$cycle  $($started.ToString('yyyy-MM-dd HH:mm:ss')) ───────────────────"

    foreach ($t in $Tables) {
        if (-not $scripts.ContainsKey($t)) { Write-Error "알 수 없는 테이블: $t" }
        # 태그 3줄(INSERT/UPDATE/DELETE)을 한 줄로 접어 찍는다.
        $tags = Invoke-SourceScript $scripts[$t]
        Write-Host ("  {0,-9} {1}" -f $t, ($tags -join " "))
    }

    $elapsed = [int]((Get-Date) - $started).TotalSeconds

    $srcCar = Get-SourceScalar "SELECT count(*) FROM car"
    $srcCom = Get-SourceScalar "SELECT count(*) FROM computer"
    $tgtCar = Get-TargetScalar "SELECT count(*) FROM car"
    $tgtCom = Get-TargetScalar "SELECT count(*) FROM computer WHERE deleted = false"

    Write-Host "  소요 ${elapsed}s | source car=$srcCar computer=$srcCom | target(직후) car=$tgtCar computer=$tgtCom"

    $cycle++
    if ($Cycles -ne 0 -and $cycle -gt $Cycles) { break }

    # 작업 시간을 뺀 만큼만 잔다 — 사이클이 5분 경계에서 밀리지 않게 한다.
    $remain = [Math]::Max($IntervalSec - $elapsed, 1)
    Write-Host "  다음 사이클까지 ${remain}s 대기"
    Write-Host ""
    Start-Sleep -Seconds $remain
}

Write-Host ""
Write-Host "완료 — $($cycle - 1) 사이클"

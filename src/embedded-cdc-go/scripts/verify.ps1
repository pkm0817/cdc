# 캡처 신뢰성 검증 테스트(V1~V6) 실행.
# 기동 중인 source/target PostgreSQL 에 붙어 verify_* 전용 테이블·publication·슬롯으로만 돈다.
# 운영 테이블(car, computer, grade, member)과 embedded_cdc_go_slot 은 건드리지 않는다.
param(
    [string]$Run,            # 예: -Run "V3"  (특정 시나리오만)
    [string]$SourceUrl = "postgres://postgres:postgres@localhost:57432/sourcedb?sslmode=disable",
    [string]$TargetUrl = "postgres://postgres:postgres@localhost:57433/targetdb?sslmode=disable"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$app  = Join-Path $root "dev/cdc-service"

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "go 를 찾지 못했다. Go 1.25 이상을 설치하고 PATH 에 넣을 것"
}

# 테스트는 실행 중인 DB 를 전제로 한다. 안 떠 있으면 한참 뒤에야 실패한다
$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }
$running = & $engine ps --format "{{.Names}}"
foreach ($c in @("emb-cdc-go-source-pg", "emb-cdc-go-target-pg")) {
    if ($running -notcontains $c) { throw "$c 가 떠 있지 않다. 먼저 ./scripts/up.ps1 을 실행할 것" }
}

$env:CDC_VERIFY_SOURCE_URL = $SourceUrl
$env:CDC_VERIFY_TARGET_URL = $TargetUrl
$env:CDC_VERIFY_REPORT     = Join-Path $app "build/verification/results.md"

# -count=1 : 캐시된 결과를 재사용하지 않는다. 실제 DB 상태에 따라 결과가 달라지는 테스트다.
# -p 1     : 패키지를 순차 실행한다. 슬롯과 WAL 을 다투면 계측치가 뒤섞인다.
$goArgs = @("test", "./test/verification/...", "-v", "-count=1", "-p", "1", "-timeout", "30m")
if ($Run) { $goArgs += @("-run", $Run) }

Push-Location $app
try {
    & go @goArgs
    $code = $LASTEXITCODE
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "── 검증 산출물 ───────────────────────────────────"
Write-Host "  계측 리포트 : $env:CDC_VERIFY_REPORT"
exit $code

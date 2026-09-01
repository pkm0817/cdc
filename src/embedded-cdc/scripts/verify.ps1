# 캡처 신뢰성 검증 테스트(V1~V6) 실행.
# 기동 중인 source/target PostgreSQL 에 붙어 verify_* 전용 테이블·publication·슬롯으로만 돈다.
# 운영 테이블(car, computer)과 embedded_cdc_slot 은 건드리지 않는다.
param(
    [string]$Tests,          # 예: -Tests "V3*"  (특정 시나리오만)
    [string]$SourceUrl = "jdbc:postgresql://localhost:56432/sourcedb",
    [string]$TargetUrl = "jdbc:postgresql://localhost:56433/targetdb"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$app  = Join-Path $root "dev/cdc-service"

# java 가 PATH 에 없는 환경이라 JDK 21 을 직접 찾아 준다
if (-not $env:JAVA_HOME) {
    $jdk = Get-ChildItem "$HOME/.jdks" -Directory -ErrorAction SilentlyContinue |
           Where-Object { $_.Name -like "*-21.*" } |
           Sort-Object Name -Descending | Select-Object -First 1
    if (-not $jdk) { throw "JDK 21 을 찾지 못했다. `$env:JAVA_HOME 을 직접 지정할 것" }
    $env:JAVA_HOME = $jdk.FullName
}
Write-Host "JAVA_HOME = $($env:JAVA_HOME)"

# 테스트는 실행 중인 DB 를 전제로 한다. 안 떠 있으면 gradle 이 한참 뒤에야 실패한다
$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }
$running = & $engine ps --format "{{.Names}}"
foreach ($c in @("emb-cdc-source-pg", "emb-cdc-target-pg")) {
    if ($running -notcontains $c) { throw "$c 가 떠 있지 않다. 먼저 ./scripts/up.ps1 을 실행할 것" }
}

$gradleArgs = @("test", "--console=plain",
                "-Dcdc.verify.source.url=$SourceUrl",
                "-Dcdc.verify.target.url=$TargetUrl")
if ($Tests) { $gradleArgs += @("--tests", $Tests) }

Push-Location $app
try {
    & "$app/gradlew.bat" @gradleArgs
    $code = $LASTEXITCODE
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "── 검증 산출물 ───────────────────────────────────"
Write-Host "  계측 리포트 : $app/build/verification/results.md"
Write-Host "  테스트 리포트: $app/build/reports/tests/test/index.html"
exit $code

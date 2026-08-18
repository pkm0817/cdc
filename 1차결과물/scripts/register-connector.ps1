# Kafka Connect 가 뜬 뒤 Debezium PostgreSQL 커넥터를 등록한다.
# 사용: .\scripts\register-connector.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$configPath = Join-Path $root 'connector\member-pg-connector.json'
$connect = 'http://localhost:8084'   # 호스트 8083 충돌을 피해 8084 로 노출

Write-Host 'Kafka Connect 기동 대기 중...' -ForegroundColor Cyan
$ready = $false
foreach ($i in 1..60) {
    try {
        Invoke-RestMethod -Uri "$connect/connectors" -TimeoutSec 3 | Out-Null
        $ready = $true
        break
    } catch {
        Start-Sleep -Seconds 3
    }
}
if (-not $ready) { throw "Kafka Connect 가 응답하지 않습니다 ($connect)" }

Write-Host '커넥터 등록 중...' -ForegroundColor Cyan
$body = Get-Content $configPath -Raw -Encoding UTF8
try {
    Invoke-RestMethod -Uri "$connect/connectors" -Method Post `
        -ContentType 'application/json' -Body $body | Out-Null
    Write-Host '등록 완료' -ForegroundColor Green
} catch {
    if ($_.Exception.Response.StatusCode.value__ -eq 409) {
        Write-Host '이미 등록되어 있습니다' -ForegroundColor Yellow
    } else {
        throw
    }
}

Start-Sleep -Seconds 3
$status = Invoke-RestMethod -Uri "$connect/connectors/member-pg-connector/status"
Write-Host ''
Write-Host "connector : $($status.connector.state)"
foreach ($t in $status.tasks) { Write-Host "task[$($t.id)]  : $($t.state)" }
$failed = $status.tasks | Where-Object { $_.state -ne 'RUNNING' }
if ($failed) {
    Write-Host ''
    Write-Host '실패한 task 의 trace:' -ForegroundColor Red
    $failed.trace
}

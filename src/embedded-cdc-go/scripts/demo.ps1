# CDC 동작 데모: source 에 INSERT/UPDATE/DELETE 를 넣고 target 반영을 확인
$ErrorActionPreference = "Stop"

$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }

function Invoke-SourceSql([string]$sql) {
    & $engine exec emb-cdc-go-source-pg psql -U postgres -d sourcedb -c $sql
}
function Invoke-TargetSql([string]$sql) {
    & $engine exec emb-cdc-go-target-pg psql -U postgres -d targetdb -c $sql
}

Write-Host "── 1. INSERT ──────────────────────────────────────"
Invoke-SourceSql "INSERT INTO car (name, brand, price) VALUES ('Ioniq 5', 'Hyundai', 52000000.00);"
Invoke-SourceSql "INSERT INTO computer (brand, model, cpu, ram_gb, price_usd) VALUES ('ASUS', 'ROG Zephyrus', 'Ryzen 9', 32, 2299.00);"

Write-Host "── 2. UPDATE ──────────────────────────────────────"
Invoke-SourceSql "UPDATE car SET price = price * 0.95, updated_at = now() WHERE name = 'Avante';"
Invoke-SourceSql "UPDATE computer SET ram_gb = 48 WHERE model = 'ThinkPad X1';"

Write-Host "── 3. DELETE ──────────────────────────────────────"
Invoke-SourceSql "DELETE FROM car WHERE name = 'K5';"
Invoke-SourceSql "DELETE FROM computer WHERE model = 'XPS 13';"

Write-Host "── 4. 복제 대기 (3초) ─────────────────────────────"
Start-Sleep -Seconds 3

Write-Host "── 5. source 상태 ─────────────────────────────────"
Invoke-SourceSql "SELECT id, name, brand, price FROM car ORDER BY id;"
Invoke-SourceSql "SELECT id, brand, model, ram_gb, price_usd FROM computer ORDER BY id;"

Write-Host "── 6. target 상태 ─────────────────────────────────"
Invoke-TargetSql "SELECT id, name, brand, price FROM car ORDER BY id;"
Invoke-TargetSql "SELECT id, full_name, spec, price_krw, deleted, source_lsn FROM computer ORDER BY id;"

Write-Host "── 7. 행 수 정합성 ────────────────────────────────"
Invoke-SourceSql "SELECT 'car' AS t, count(*) FROM car UNION ALL SELECT 'computer', count(*) FROM computer;"
Invoke-TargetSql "SELECT 'car' AS t, count(*) FROM car UNION ALL SELECT 'computer(active)', count(*) FROM computer WHERE deleted = false;"

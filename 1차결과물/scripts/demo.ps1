# PostgreSQL 을 변경하고 MySQL 에 반영되는지 확인한다.
# 사용: .\scripts\demo.ps1

$ErrorActionPreference = 'Stop'

function Invoke-Pg([string]$sql) {
    docker exec -i cdc-postgres psql -U postgres -d memberdb -c $sql
}
function Show-MySql {
    docker exec -i cdc-mysql mysql -uroot -proot userdb `
        -e 'SELECT id, name, email, user_status, deleted, source_lsn FROM `user` ORDER BY id;' 2>$null
}

Write-Host '── 현재 MySQL 상태 (snapshot 결과) ──' -ForegroundColor Cyan
Show-MySql

Write-Host ''
Write-Host '── UPDATE: 홍길동의 이메일 변경 ──' -ForegroundColor Cyan
Invoke-Pg "UPDATE members SET email_address = 'gildong-new@example.com', updated_at = now() WHERE member_id = 1;"
Start-Sleep -Seconds 3
Show-MySql

Write-Host ''
Write-Host '── INSERT: 신규 회원 ──' -ForegroundColor Cyan
Invoke-Pg "INSERT INTO members (full_name, email_address, status) VALUES ('이영희', 'younghee@example.com', 'ACTIVE');"
Start-Sleep -Seconds 3
Show-MySql

Write-Host ''
Write-Host '── DELETE: 김철수 삭제 → 소프트 삭제로 반영 (deleted=1) ──' -ForegroundColor Cyan
Invoke-Pg "DELETE FROM members WHERE member_id = 2;"
Start-Sleep -Seconds 3
Show-MySql

#!/usr/bin/env sh
# PostgreSQL 을 변경하고 MySQL 에 반영되는지 확인한다 (WSL/Linux 용).
# 사용: sh scripts/demo.sh

pg() {
    docker exec cdc-postgres psql -U postgres -d memberdb -c "$1" >/dev/null
}
target() {
    docker exec cdc-mysql mysql -uroot -proot userdb --default-character-set=utf8mb4 --table -e \
        'SELECT id, name, email, user_status, deleted, source_lsn FROM `user` ORDER BY id;' 2>/dev/null
}

echo '── 현재 상태 (snapshot 결과) ──'
target

echo ''
echo '── UPDATE: 홍길동의 이메일 변경 ──'
pg "UPDATE members SET email_address = 'gildong-new@example.com', updated_at = now() WHERE member_id = 1;"
sleep 4
target

echo ''
echo '── INSERT: 신규 회원 이영희 ──'
pg "INSERT INTO members (full_name, email_address, status) VALUES ('이영희', 'younghee@example.com', 'ACTIVE');"
sleep 4
target

echo ''
echo '── DELETE: 김철수 삭제 → 소프트 삭제(deleted=1)로 반영 ──'
pg "DELETE FROM members WHERE member_id = 2;"
sleep 4
target

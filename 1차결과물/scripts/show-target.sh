#!/usr/bin/env sh
# MySQL 대상 테이블의 현재 상태를 출력한다.
docker exec cdc-mysql mysql -uroot -proot userdb --table -e \
  'SELECT id, name, email, user_status, deleted, source_lsn FROM `user` ORDER BY id;'

package com.example.membersync.adapter.out.persistence;

import com.example.membersync.application.port.out.UserSyncPort;
import com.example.membersync.domain.UserSnapshot;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 아웃바운드 어댑터.
 *
 * <p>UPSERT 한 문장이 두 가지를 동시에 막는다:
 * <ul>
 *   <li><b>멱등성</b> — Debezium/Kafka 는 at-least-once 이므로 같은 이벤트가 재전달되는 것이
 *       정상이다. 몇 번 적용해도 결과가 같아야 한다.</li>
 *   <li><b>순서 역전</b> — source_lsn 이 더 낮은(=더 오래된) 이벤트가 늦게 도착하면 모든
 *       컬럼이 기존 값으로 유지된다.</li>
 * </ul>
 *
 * <p>{@code VALUES()} 함수는 MySQL 8.0.20+ 에서 deprecated 이므로 {@code AS new} 별칭을 쓴다.
 */
@Repository
public class UserSyncAdapter implements UserSyncPort {

    private static final String UPSERT = """
            INSERT INTO `user` (id, name, email, user_status, deleted, source_lsn, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, NOW(3)) AS new
            ON DUPLICATE KEY UPDATE
              name        = IF(new.source_lsn > `user`.source_lsn, new.name,        `user`.name),
              email       = IF(new.source_lsn > `user`.source_lsn, new.email,       `user`.email),
              user_status = IF(new.source_lsn > `user`.source_lsn, new.user_status, `user`.user_status),
              deleted     = IF(new.source_lsn > `user`.source_lsn, new.deleted,     `user`.deleted),
              synced_at   = IF(new.source_lsn > `user`.source_lsn, NOW(3),          `user`.synced_at),
              source_lsn  = GREATEST(new.source_lsn, `user`.source_lsn)
            """;

    private final JdbcTemplate jdbcTemplate;

    public UserSyncAdapter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void upsert(UserSnapshot user) {
        jdbcTemplate.update(UPSERT,
                user.id(),
                user.name(),
                user.email(),
                user.status(),
                user.deleted(),
                user.sourceLsn());
    }
}

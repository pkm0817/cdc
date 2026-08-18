package com.example.membersync.application;

import com.example.membersync.adapter.in.kafka.MemberChangeEvent;
import com.example.membersync.application.port.in.SyncMemberUseCase;
import com.example.membersync.application.port.out.UserSyncPort;
import com.example.membersync.domain.UserSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 도메인. op 분기와 이종 스키마 매핑이 여기서 실행된다. */
@Service
public class SyncMemberService implements SyncMemberUseCase {

    private static final Logger log = LoggerFactory.getLogger(SyncMemberService.class);

    private final UserSyncPort userSyncPort;

    public SyncMemberService(UserSyncPort userSyncPort) {
        this.userSyncPort = userSyncPort;
    }

    @Override
    @Transactional
    public void sync(MemberChangeEvent event) {
        long lsn = event.source().lsn();

        switch (event.op()) {
            // r = snapshot read, c = insert, u = update → 전부 같은 UPSERT.
            // snapshot 이 재실행돼도 대상이 깨지지 않는 이유이기도 하다.
            case "r", "c", "u" -> userSyncPort.upsert(toUser(event.after(), lsn, false));

            // d = delete → after 가 null 이므로 before 에서 값을 꺼낸다.
            // before 가 PK 외 컬럼까지 채워져 있는 것은 REPLICA IDENTITY FULL 덕분이다.
            case "d" -> userSyncPort.upsert(toUser(event.before(), lsn, true));

            default -> throw new IllegalArgumentException("unknown op: " + event.op());
        }

        // wiki 의 SLI 정의: Sink write 시각 − source.ts_ms
        log.info("synced op={} lsn={} e2e={}ms",
                event.op(), lsn, System.currentTimeMillis() - event.source().tsMs());
    }

    /** PostgreSQL members → MySQL user 스키마 매핑. 컬럼명이 전부 다르다. */
    private UserSnapshot toUser(MemberChangeEvent.MemberRow row, long lsn, boolean deleted) {
        return new UserSnapshot(
                row.memberId(),        // member_id     → id
                row.fullName(),        // full_name     → name
                row.emailAddress(),    // email_address → email
                row.status(),          // status        → user_status
                deleted,
                lsn
        );
    }
}

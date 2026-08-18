package com.example.membersync.application.port.in;

import com.example.membersync.adapter.in.kafka.MemberChangeEvent;

/** 인바운드 포트. 어댑터는 이것을 "호출"만 하고, 로직은 구현체에서 "실행"된다. */
public interface SyncMemberUseCase {

    void sync(MemberChangeEvent event);
}

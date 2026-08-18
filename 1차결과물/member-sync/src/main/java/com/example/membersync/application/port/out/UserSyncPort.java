package com.example.membersync.application.port.out;

import com.example.membersync.domain.UserSnapshot;

/** 아웃바운드 포트. 대상 저장소가 무엇이든 도메인은 이 계약만 안다. */
public interface UserSyncPort {

    void upsert(UserSnapshot user);
}

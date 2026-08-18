package com.example.membersync.domain;

/** MySQL {@code user} 테이블에 반영할 한 행의 상태. */
public record UserSnapshot(
        Long id,
        String name,
        String email,
        String status,
        boolean deleted,
        long sourceLsn
) {}

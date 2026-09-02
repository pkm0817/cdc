package dev.embeddedcdc.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * source PostgreSQL 접속과 Debezium 캡처 범위 설정.
 * 값은 application.yml 의 cdc.source.* 에서 오고, 환경변수로 덮어쓸 수 있다.
 *
 * name 과 topicPrefix 는 인스턴스마다 달라야 하는 값이라 설정으로 뺐다 —
 * 같은 DB 에 커넥터를 둘 이상 붙일 때 이 둘이 겹치면 오프셋과 지표 이름이 충돌한다.
 */
@ConfigurationProperties(prefix = "cdc.source")
public record CdcSourceProperties(
        String name,
        String topicPrefix,
        String hostname,
        int port,
        String user,
        String password,
        String dbname,
        String slotName,
        String publicationName,
        String tableIncludeList,
        String snapshotMode,
        String offsetFile,
        /**
         * heartbeat 주기(ms). 0 이면 끈다.
         * 관심 테이블에 변경이 없어도 슬롯을 전진시키기 위한 것이다.
         */
        int heartbeatIntervalMs,
        /**
         * heartbeat 시각에 실행할 쿼리. publication 에 든 테이블에 쓰기를 만들어야
         * confirmed_flush_lsn 이 전진한다 — interval 만으로는 0 bytes 였다 (V6).
         */
        String heartbeatActionQuery,
        /**
         * 캡처 연결고리가 끊긴 것을 발견했을 때 기동을 거부할지.
         * false 로 두면 로그만 남기고 계속 돈다 — 어긋난 채로 도는 것을 감수하겠다는 뜻이다.
         */
        boolean failOnCaptureGap
) {
}

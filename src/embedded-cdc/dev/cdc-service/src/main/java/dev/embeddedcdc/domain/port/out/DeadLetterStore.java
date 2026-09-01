package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.PendingDeadLetter;

import java.util.List;

/**
 * 반영하지 못한 이벤트를 보관하는 곳(outbound port).
 *
 * <b>여기 들어간 것은 유실이 아니라 "추적되는 미반영"이다.</b>
 * 원인을 고친 뒤 다시 처리할 수 있어야 하므로, 재구성에 필요한 값을 전부 남긴다.
 */
public interface DeadLetterStore {

    /** 역직렬화는 됐으나 적용에 실패한 경우. */
    void store(String pipeline, ChangeEvent event, Throwable cause, int attempts);

    /** 역직렬화 자체가 안 된 경우. 도메인 객체가 없으므로 원문을 그대로 남긴다. */
    void storeUnparsable(String pipeline, String rawPayload, Throwable cause);

    /** 아직 처리되지 않은 건수. 경보의 기준이 된다. */
    long pendingCount(String pipeline);

    /**
     * 재처리 대상으로 표시된 건을 가져온다.
     *
     * <b>PENDING 을 자동으로 집지 않는 이유가 있다.</b> 원인이 고쳐졌는지는 사람만 안다.
     * 자동 재시도를 돌리면 고쳐지지 않은 독성 건이 영원히 재시도되며 잡음만 쌓인다.
     * 그래서 status 를 RETRY_REQUESTED 로 바꾸는 것이 명시적인 재처리 신청이 된다.
     */
    List<PendingDeadLetter> claimForRetry(String pipeline, int limit);

    /** 재처리 성공. */
    void markResolved(long id);

    /** 재처리 실패. 다시 PENDING 으로 돌리고 시도 횟수를 올린다. */
    void markRetryFailed(long id, Throwable cause);
}

package dev.cdccustom.infrastructure.jdbc;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 진행 지점 저장소. <b>타깃 DB</b> 에 둔다.
 *
 * <p>위치가 핵심이다. 반영(타깃 쓰기)과 진행 지점 갱신이 같은 DB, 같은 트랜잭션에
 * 들어가야 "반영은 됐는데 진행 지점은 뒤에 남는" 틈이 생기지 않는다.
 *
 * <p>CDC 판 Go 구현은 이 틈 때문에 사고가 났다. 체크포인트는 주기적으로 기록하는데
 * 슬롯 ack 는 계속 나가다 보니, 평범한 재기동에서도 둘의 차이가 "되받을 수 없는 구간"
 * 으로 오인돼 기동이 막혔다. 여기서는 둘이 한 트랜잭션이라 그 상태가 존재할 수 없다.
 */
@Component
public class CheckpointStore {

    private static final String PIPELINE = "cdc-custom";

    private static final String READ = "SELECT last_seq FROM sync_checkpoint WHERE pipeline = ?";

    private static final String ADVANCE = """
            UPDATE sync_checkpoint
               SET last_seq = ?, updated_at = now()
             WHERE pipeline = ? AND last_seq < ?
            """;

    private final JdbcTemplate target;

    public CheckpointStore(@Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        this.target = targetJdbc;
    }

    public long current() {
        Long seq = target.queryForObject(READ, Long.class, PIPELINE);
        return seq == null ? 0L : seq;
    }

    /**
     * 진행 지점을 앞으로만 옮긴다. {@code last_seq < ?} 조건이 뒤로 미는 갱신을 막는다 —
     * 재시도로 같은 배치가 두 번 들어와도 지점이 되감기지 않는다.
     */
    public void advanceTo(long seq) {
        target.update(ADVANCE, seq, PIPELINE, seq);
    }
}

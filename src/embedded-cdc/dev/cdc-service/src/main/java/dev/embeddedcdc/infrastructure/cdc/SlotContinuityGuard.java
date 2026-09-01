package dev.embeddedcdc.infrastructure.cdc;

import dev.embeddedcdc.domain.port.out.CheckpointStore;
import dev.embeddedcdc.infrastructure.config.CdcSourceProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 캡처 연결고리가 끊겼는지 기동 시점에 판정한다.
 *
 * <b>왜 필요한가.</b> Debezium 은 "오프셋 파일은 있는데 슬롯이 없다"는 상황은 스스로 잡아낸다
 * (V3 검증에서 확인: 기동을 거부하고 오류로 종료한다). 하지만 그 판단의 근거가 오프셋 파일이라
 * <b>오프셋 파일이 함께 사라지면 아무것도 알아채지 못한다</b> —
 * 볼륨을 지우고 재기동하면 Debezium 은 그저 최초 기동으로 보고 새 슬롯을 만든다.
 * 그 사이 변경분은 조용히 사라진다.
 *
 * 그래서 진행 지점을 <b>수신 측 DB</b> 에도 남기고(CheckpointStore), 기동할 때 슬롯과 대조한다.
 * Provider 볼륨이 통째로 날아가도 이 기록은 살아남는다.
 *
 * 판정 규칙 — 둘 중 하나라도 걸리면 되받을 수 없는 구간이 생긴 것이다.
 *   1. 처리 이력은 있는데 슬롯이 없다
 *   2. 슬롯의 restart_lsn 이 마지막으로 처리한 지점보다 앞서 있다
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SlotContinuityGuard {

    private final CdcSourceProperties props;
    private final CheckpointStore checkpoints;

    /**
     * @return 되받을 수 없는 구간이 있으면 그 내용, 없으면 empty
     */
    public Optional<CaptureGap> detectGap() {
        OptionalLong lastApplied = checkpoints.lastAppliedLsn(props.name());
        if (lastApplied.isEmpty()) {
            log.info("진행 기록이 없다 — 최초 기동으로 본다 (pipeline={})", props.name());
            return Optional.empty();
        }

        SlotState slot = readSlotState(props.slotName());

        if (!slot.exists()) {
            return Optional.of(new CaptureGap(lastApplied.getAsLong(), null,
                    "처리 이력(LSN " + toLsnText(lastApplied.getAsLong()) + ")은 있는데 복제 슬롯 '"
                            + props.slotName() + "' 이 없다. 슬롯이 삭제됐거나 DB 가 교체됐다"));
        }

        long restart = slot.restartLsn();
        if (restart > lastApplied.getAsLong()) {
            return Optional.of(new CaptureGap(lastApplied.getAsLong(), restart,
                    "슬롯이 이미 " + toLsnText(restart) + " 까지 버렸는데 우리는 "
                            + toLsnText(lastApplied.getAsLong()) + " 까지만 처리했다. 그 사이는 되받을 수 없다"));
        }

        log.info("캡처 연결고리 정상 — 처리 지점 {} / 슬롯 restart_lsn {}",
                toLsnText(lastApplied.getAsLong()), toLsnText(restart));
        return Optional.empty();
    }

    private SlotState readSlotState(String slotName) {
        String url = "jdbc:postgresql://" + props.hostname() + ":" + props.port() + "/" + props.dbname();
        String sql = "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = ?";

        try (Connection c = DriverManager.getConnection(url, props.user(), props.password());
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return new SlotState(false, 0);
                }
                String restartLsn = rs.getString(1);
                // 슬롯은 있는데 restart_lsn 이 null 인 경우가 있다(아직 한 번도 사용되지 않은 슬롯).
                // 버린 구간이 없다는 뜻이므로 0 으로 본다.
                return new SlotState(true, restartLsn == null ? 0 : toLong(restartLsn));
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "복제 슬롯 상태를 읽지 못했다 — 연결고리를 확인할 수 없으므로 기동하지 않는다", e);
        }
    }

    /** PostgreSQL LSN 표기("0/1A2B3C4")를 비교 가능한 정수로 바꾼다. */
    public static long toLong(String lsn) {
        int slash = lsn.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("LSN 형식이 아니다: " + lsn);
        }
        long high = Long.parseUnsignedLong(lsn.substring(0, slash), 16);
        long low = Long.parseUnsignedLong(lsn.substring(slash + 1), 16);
        return (high << 32) | low;
    }

    /** 정수 LSN 을 사람이 읽는 표기로 되돌린다. 로그와 경보에서 슬롯 조회 결과와 바로 비교된다. */
    public static String toLsnText(long lsn) {
        return Long.toHexString(lsn >>> 32).toUpperCase() + "/" + Long.toHexString(lsn & 0xFFFFFFFFL).toUpperCase();
    }

    private record SlotState(boolean exists, long restartLsn) {
    }

    /**
     * @param lastAppliedLsn 우리가 마지막으로 처리한 지점
     * @param slotRestartLsn 슬롯이 보관 중인 가장 오래된 지점. 슬롯이 없으면 null
     */
    public record CaptureGap(long lastAppliedLsn, Long slotRestartLsn, String reason) {

        public String describe() {
            return reason + " — 이 구간은 WAL 로 복구할 수 없다. 관심 테이블 재적재(재동기화)가 필요하다.";
        }
    }
}

package dev.embeddedcdc.domain.port.out;

import java.util.OptionalLong;

/**
 * 파이프라인이 어디까지 처리했는지를 우리 손으로 기록하는 곳(outbound port).
 *
 * Debezium 의 오프셋 파일과 별개로 둔다. 오프셋 파일은 Provider 쪽 볼륨에 있어서
 * 볼륨이 날아가면 같이 사라지는데, 그러면 "처음부터 다시 읽는 것"과
 * "구간을 건너뛴 것"을 구분할 수 없다. 수신 측 DB 에 남겨야 그 판단이 가능하다.
 */
public interface CheckpointStore {

    /** 마지막으로 처리한 WAL 위치. 기록이 없으면 최초 기동이다. */
    OptionalLong lastAppliedLsn(String pipeline);

    /** 배치 하나를 끝낼 때마다 부른다. 이벤트마다 부르면 왕복이 두 배가 된다. */
    void record(String pipeline, long lsn);
}

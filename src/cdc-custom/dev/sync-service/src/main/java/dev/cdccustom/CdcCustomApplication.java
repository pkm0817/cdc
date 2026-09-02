package dev.cdccustom;

import dev.cdccustom.infrastructure.config.SyncProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * WAL 을 읽지 않는 동기화 서비스.
 *
 * <p>전제는 하나다 — <b>두 DB 가 이미 같은 상태에서 출발한다.</b> 그 위에서는
 * "무엇이 어떻게 바뀌었는지"를 옮길 필요가 없고, "어느 행이 손대졌는지"만 알면
 * 소스의 현재 값을 읽어 맞추면 된다.
 *
 * <p>얻는 것: 기록량이 CDC 의 3% 수준, 같은 행 반복 변경은 한 번으로 접힘,
 * 복제 슬롯이 없으니 원천 디스크 고갈 위험도 없음.
 * <p>잃는 것: 중간 상태. 변경 이력·감사가 필요하면 이 방식으로는 답할 수 없다.
 */
@SpringBootApplication
@EnableConfigurationProperties(SyncProperties.class)
public class CdcCustomApplication {

    public static void main(String[] args) {
        SpringApplication.run(CdcCustomApplication.class, args);
    }
}

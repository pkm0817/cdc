package dev.cdccustom.infrastructure.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * DataSource 두 개 — 소스(읽기)와 타깃(쓰기).
 *
 * <p>이 서비스는 소스에 업무 데이터를 <b>쓰지 않는다.</b> 읽는 것은 outbox 와 업무 표의
 * 현재 값이고, 쓰는 것은 outbox 비우기 하나다. 그래서 트랜잭션 매니저는 타깃 것만 의미가 있다.
 *
 * <p><b>모든 주입 지점에 {@code @Qualifier} 를 붙인다.</b> 같은 타입 빈이 둘이면 스프링은
 * 파라미터 이름으로 고르려 하는데, 그 이름은 컴파일 옵션에 따라 사라질 수 있다. 그러면
 * 조용히 {@code @Primary}(=타깃)가 주입된다. 실제로 이 서비스는 그 상태로 기동해
 * "소스용" JdbcTemplate 이 타깃 DB 를 보는 바람에 outbox 를 찾지 못했다.
 * 이름에 기대지 않고 한정자로 못박는다.
 *
 * <p>{@code reWriteBatchedInserts=true} 를 타깃 URL 에 켠다. pgjdbc 가 배치로 모인 INSERT 를
 * 다중 VALUES 한 문장으로 다시 써 보내므로, 5,000행 배치의 왕복이 몇 번으로 줄어든다.
 */
@Configuration
public class DataSourceConfig {

    @Bean
    @ConfigurationProperties("datasource.source")
    public DataSourceProperties sourceDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("datasource.target")
    public DataSourceProperties targetDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    public DataSource sourceDataSource(
            @Qualifier("sourceDataSourceProperties") DataSourceProperties props) {
        return props.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    @Primary
    public DataSource targetDataSource(
            @Qualifier("targetDataSourceProperties") DataSourceProperties props) {
        return props.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    public JdbcTemplate sourceJdbc(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @Primary
    public JdbcTemplate targetJdbc(@Qualifier("targetDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /** 반영과 체크포인트를 한 트랜잭션으로 묶는 매니저. 이름을 BatchApplier 가 참조한다. */
    @Bean
    public PlatformTransactionManager targetTransactionManager(
            @Qualifier("targetDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}

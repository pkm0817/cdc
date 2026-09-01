package dev.embeddedcdc.infrastructure.config;

import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * QueryDSL 진입점.
 *
 * JPAQueryFactory 는 EntityManager 를 감싸는 얇은 래퍼다. 여기서 주입받는 EntityManager 는
 * 스프링이 만들어 주는 프록시라 스레드마다 현재 트랜잭션의 진짜 EntityManager 로 연결된다 —
 * 그래서 이 팩토리를 싱글턴 빈으로 두어도 안전하다.
 */
@Configuration
public class QuerydslConfig {

    @Bean
    public JPAQueryFactory jpaQueryFactory(EntityManager entityManager) {
        return new JPAQueryFactory(entityManager);
    }
}

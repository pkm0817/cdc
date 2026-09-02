plugins {
    java
    id("org.springframework.boot") version "3.5.4"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "dev.cdccustom"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // JPA 를 쓰지 않는다. 이 서비스가 하는 일은 "PK 목록으로 읽어 벌크로 쓰기" 하나뿐이고,
    // 영속성 컨텍스트·더티 체킹은 그 경로에서 비용만 된다. JdbcTemplate 의 배치 갱신이
    // 목적에 정확히 맞는다. (CDC 판은 이벤트 단위 도메인 모델이 필요해 JPA 를 썼다)
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

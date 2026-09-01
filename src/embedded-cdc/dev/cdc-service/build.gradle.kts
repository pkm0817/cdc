plugins {
    java
    id("org.springframework.boot") version "3.5.4"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "dev.embeddedcdc"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

val debeziumVersion = "3.6.1.Final"
val querydslVersion = "5.1.0"

dependencies {
    // 버전은 Spring Boot BOM 이 관리한다. compileOnly 라 실행 시점 의존성에는 남지 않는다.
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    // QueryDSL — jakarta 분류자를 반드시 붙인다. 기본 아티팩트는 javax 시절 것이라
    // Spring Boot 3 / Hibernate 6 에서 클래스를 찾지 못한다.
    implementation("com.querydsl:querydsl-jpa:$querydslVersion:jakarta")
    annotationProcessor("com.querydsl:querydsl-apt:$querydslVersion:jakarta")
    annotationProcessor("jakarta.annotation:jakarta.annotation-api")
    annotationProcessor("jakarta.persistence:jakarta.persistence-api")

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")

    implementation("io.debezium:debezium-api:$debeziumVersion")
    implementation("io.debezium:debezium-embedded:$debeziumVersion") {
        // debezium-embedded 가 끌고 오는 옛 log4j 계열 바인딩이 Boot 의 logback 과 충돌하는 것을 방지
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
    }
    implementation("io.debezium:debezium-connector-postgres:$debeziumVersion")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility")
    testImplementation("org.postgresql:postgresql")
    testCompileOnly("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
}

tasks.withType<Test> {
    useJUnitPlatform()

    // 검증 테스트는 실제로 기동 중인 source/target PostgreSQL 에 붙는다.
    // 기본값은 embedded-cdc 스택의 포트이며, 다른 환경이면 -D 로 덮어쓴다.
    systemProperty("cdc.verify.source.url",
        System.getProperty("cdc.verify.source.url", "jdbc:postgresql://localhost:56432/sourcedb"))
    systemProperty("cdc.verify.target.url",
        System.getProperty("cdc.verify.target.url", "jdbc:postgresql://localhost:56433/targetdb"))
    systemProperty("cdc.verify.user", System.getProperty("cdc.verify.user", "postgres"))
    systemProperty("cdc.verify.password", System.getProperty("cdc.verify.password", "postgres"))

    // 계측치를 한 파일로 모은다. 검증 보고서의 원자료가 된다.
    systemProperty("cdc.verify.report", layout.buildDirectory.file("verification/results.md").get().asFile.path)

    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

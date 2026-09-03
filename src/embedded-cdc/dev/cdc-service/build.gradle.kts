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

    // V4-b 만 기동 중인 서비스 자체를 쓴다 — 운영 DLQ 재처리기와 테이블별 가드가 검증 대상이라
    // 복제본으로는 답이 나오지 않는다. 파이프라인 이름은 cdc.source.name 과 같아야 한다.
    systemProperty("cdc.verify.pipeline", System.getProperty("cdc.verify.pipeline", "embedded-cdc"))
    systemProperty("cdc.verify.health.url",
        System.getProperty("cdc.verify.health.url", "http://localhost:56080/actuator/health"))

    // V5-b 의 1단계 선별 질의. 테스트가 SQL 을 따로 들고 있으면 운영자가 손으로 돌리는 것과
    // 갈라진다 — 검증에서 통과한 질의와 실제로 쓰는 질의는 같은 파일이어야 한다.
    systemProperty("cdc.verify.toast.sql",
        layout.projectDirectory.file("../../scripts/toast-candidates.sql").asFile.path)

    // V6-b 도 같은 이유로 운영이 쓰는 파일을 그대로 읽는다 — 대사 질의와 고아 참조 질의.
    systemProperty("cdc.verify.reconcile.sql",
        layout.projectDirectory.file("../../scripts/reconcile.sql").asFile.path)
    systemProperty("cdc.verify.exporter.target.yaml",
        layout.projectDirectory.file("../../infra/monitoring/exporter/queries-target.yaml").asFile.path)

    // 계측치를 한 파일로 모은다. 검증 보고서의 원자료가 된다.
    systemProperty("cdc.verify.report", layout.buildDirectory.file("verification/results.md").get().asFile.path)

    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

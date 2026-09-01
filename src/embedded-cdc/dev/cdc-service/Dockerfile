# ── 1단계: 빌드 ──────────────────────────────────────────────────────────────
FROM gradle:8.14-jdk21 AS build
WORKDIR /workspace

# 의존성 캐시를 살리기 위해 빌드 스크립트를 먼저 복사
# lombok.config 를 빠뜨리면 컨테이너 빌드에서만 Lombok 설정이 무시된다 —
# @Data 금지 같은 규칙이 로컬에서는 걸리는데 CI 에서는 통과하는 상황이 된다.
COPY build.gradle.kts settings.gradle.kts lombok.config ./
RUN gradle dependencies --no-daemon > /dev/null 2>&1 || true

COPY src ./src
RUN gradle bootJar --no-daemon

# ── 2단계: 실행 ──────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre
WORKDIR /app

# compose healthcheck 용
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/build/libs/*.jar app.jar

# Debezium offset 파일 저장 위치 (compose 에서 named volume 마운트)
RUN mkdir -p /app/data
ENV CDC_OFFSET_FILE=/app/data/offsets.dat

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]

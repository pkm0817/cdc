# ── 1단계: 빌드 ──────────────────────────────────────────────────────────────
FROM gradle:8.14-jdk21 AS build
WORKDIR /workspace

# 사내망이 TLS 를 가로채 검사하면(예: Somansa Root CA) 컨테이너 안에서 Maven 저장소로
# 가는 HTTPS 가 신뢰 실패로 끊긴다. 호스트 Windows 는 인증서를 갖고 있지만 컨테이너는
# 모르기 때문이다. certs/ 에 인증서가 있으면 OS 와 JDK 양쪽 트러스트에 넣는다.
# Gradle 은 JDK 트러스트를 보므로 update-ca-certificates 만으로는 부족하다 —
# keytool 로 cacerts 에도 넣어야 한다.
# 폴더가 비어 있으면 아무 일도 하지 않는다(사내망 밖에서는 필요 없다).
COPY certs/ /tmp/certs/
RUN set -eu; \
    if ls /tmp/certs/*.crt >/dev/null 2>&1; then \
      cp /tmp/certs/*.crt /usr/local/share/ca-certificates/; \
      update-ca-certificates; \
      for c in /tmp/certs/*.crt; do \
        keytool -importcert -noprompt -trustcacerts \
                -alias "$(basename "$c" .crt)" -file "$c" -cacerts -storepass changeit; \
      done; \
    fi

# 의존성 캐시를 살리기 위해 빌드 스크립트를 먼저 복사
COPY build.gradle.kts settings.gradle.kts ./
RUN gradle dependencies --no-daemon > /dev/null 2>&1 || true

COPY src ./src
RUN gradle bootJar --no-daemon

# ── 2단계: 실행 ──────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre
WORKDIR /app

# compose healthcheck 용
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/build/libs/*.jar app.jar

# CDC 판과 달리 오프셋 파일이 없다. 진행 지점은 타깃 DB 의 sync_checkpoint 한 줄뿐이라
# 볼륨을 붙일 이유가 없다 — 컨테이너를 지웠다 만들어도 이어서 돈다.
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]

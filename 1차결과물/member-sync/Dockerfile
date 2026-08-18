# 로컬에 JDK/Gradle이 없어도 되도록 빌드까지 컨테이너에서 수행한다
FROM gradle:8.10-jdk21 AS build
WORKDIR /src
COPY --chown=gradle:gradle settings.gradle.kts build.gradle.kts ./
COPY --chown=gradle:gradle src ./src
RUN gradle bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

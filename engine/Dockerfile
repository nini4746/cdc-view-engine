# 멀티스테이지: 빌드 스테이지에서 Gradle로 bootJar 생성, 런타임 스테이지는 JRE만
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

# 래퍼와 빌드 스크립트 먼저 복사 (의존성 캐시 활용)
COPY gradlew ./
COPY gradle ./gradle
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY engine/build.gradle.kts engine/build.gradle.kts
COPY simulator/build.gradle.kts simulator/build.gradle.kts
RUN chmod +x gradlew && ./gradlew :engine:dependencies --no-daemon > /dev/null 2>&1 || true

# 소스 복사 후 bootJar
COPY engine/src engine/src
RUN ./gradlew :engine:bootJar --no-daemon

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
# curl은 compose healthcheck에서 actuator/health 확인에 사용
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /app/data /app/views
COPY --from=build /workspace/engine/build/libs/engine.jar /app/engine.jar
# 뷰 정의는 compose에서 볼륨 마운트하지만, 이미지에도 기본 포함
COPY views /app/views
ENV CDCVIEW_VIEWS_DIR=/app/views
ENV KAFKA_STATE_DIR=/app/data/kafka-streams
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/engine.jar"]

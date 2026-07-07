# 시뮬레이터: 경량 JDBC 앱. application 플러그인의 installDist로 실행 스크립트 + 의존성 구성.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY gradlew ./
COPY gradle ./gradle
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY engine/build.gradle.kts engine/build.gradle.kts
COPY simulator/build.gradle.kts simulator/build.gradle.kts
RUN chmod +x gradlew
COPY simulator/src simulator/src
RUN ./gradlew :simulator:installDist --no-daemon

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build /workspace/simulator/build/install/simulator /app/simulator
ENTRYPOINT ["/app/simulator/bin/simulator"]

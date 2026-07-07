# Plan 01: 프로젝트 골격 + docker compose 인프라

## 목표

Spring Boot 프로젝트 골격과 docker compose 인프라 (PG + Kafka + Kafka Connect/Debezium + 엔진) 를 구축한다. SPEC R10의 인프라 구동 기준 (AC10-1, AC10-2) 을 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine (현재 README.md만 존재, 그린필드)
- 스택: Spring Boot 3.x (Java 21), Kafka Streams, Debezium (Kafka Connect), PostgreSQL. Gradle (Kotlin DSL) 멀티모듈: 루트에 `engine/` (메인 서버), `simulator/` (Plan 10에서 구현, 지금은 모듈 선언만 하거나 생략 가능)
- SPEC: /Users/yunho/project/cdc-view-engine/.planning/SPEC.md
- 아키텍처: 단일 엔진 인스턴스, RocksDB state store, 동일 프로세스 REST 서빙
- 베이스 패키지: io.github.nini4746.cdcview

## 작업

1. Gradle 프로젝트 초기화: settings.gradle.kts, engine 모듈 build.gradle.kts (spring-boot-starter-web, spring-boot-starter-actuator, kafka-streams, jackson-dataformat-yaml, postgresql 드라이버, testcontainers 테스트 의존성)
2. 엔진 메인 클래스 + application.yml (Kafka 부트스트랩, Connect URL, PG 접속 정보를 환경변수로 오버라이드 가능하게)
3. engine/Dockerfile (멀티스테이지 빌드)
4. docker-compose.yml 작성:
   - postgres (wal_level=logical 설정, 초기화 SQL: 데모 스키마 orders/inventory/settlements 테이블 + REPLICA IDENTITY FULL, docker/postgres/init.sql)
   - kafka (KRaft 단일 노드)
   - connect (debezium/connect 이미지, kafka 의존)
   - engine (빌드 컨텍스트 로컬, postgres/kafka/connect healthy 대기)
   - 각 서비스 healthcheck 정의
5. actuator health 엔드포인트 노출 설정
6. .gitignore, 최소 실행 방법을 README에 추가 (데모 서사 확장은 Plan 10)

## must-haves

- compose 파일 하나로 인프라 4개 서비스가 전부 뜬다 (수동 순서 조작 불필요)
- PG는 logical replication 활성 상태로 기동
- 데모 3테이블 (orders, inventory, settlements) 이 초기화 SQL로 생성되고 REPLICA IDENTITY FULL 적용
- 엔진 이미지가 compose 빌드로 만들어진다 (수동 jar 복사 없음)

## 검증

- [ ] AC10-1: `docker compose up -d` 후 `docker compose ps` 에서 postgres, kafka, connect, engine 모두 healthy
- [ ] AC10-2: `curl -f http://localhost:8080/actuator/health` 가 200 반환
- 보조 확인 (SPEC 외): `docker exec` 로 PG 접속해 `SHOW wal_level` = logical, 3테이블 존재 확인

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
docker compose up -d --build
docker compose ps
curl -f http://localhost:8080/actuator/health
```

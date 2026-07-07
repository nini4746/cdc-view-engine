# CDC 실시간 구체화 뷰 엔진

> 본 포트폴리오 1번. 기반 인프라. 로드맵 순서 첫 번째.

## 컨셉

DB 변경(CDC, Debezium) → Kafka Streams로 조인/집계 → 항상 최신인 구체화 뷰(materialized view)를 API로 제공 + 캐시 자동 무효화.

예시 시나리오: 주문 + 재고 + 정산 테이블 3개가 각자 바뀌는데, "판매자 대시보드 집계"는 항상 1초 내 최신.

## 왜 이거

- 희소성: GitHub 유사 레포 **1개** (2026-07-07 검증, `cdc materialized view real-time` 검색)
- 한국 JD: CDC는 토스/우아한형제들 기술블로그 단골 주제. Kafka Streams + exactly-once = 면접 화두 그 자체
- 스프링 순도 높음: 도메인 로직 전부 서버에 있음
- 킵한 2번(이상거래 엔진)과 기술 스택이 겹쳐 학습 시너지, 도메인은 완전 다름

## 핵심 난제 (= README 서사)

- 순서 뒤집힌 이벤트 처리
- 중복 전달 (exactly-once 보장)
- 스키마 변경 중 뷰 재구축
- 뷰와 원본 DB 정합성 검증

## 스택

Spring Boot, Kafka Streams, Debezium, PostgreSQL

## 2번 프로젝트와의 관계

분리 전략 채택: 이 엔진을 독립 범용 인프라로 만들고, 티켓팅 이상거래 엔진이 이걸 피처 스토어로 가져다 쓰는 첫 고객. "내가 만든 인프라를 내가 소비" 스토리. 레포 2개 확보.

이상거래 엔진이 필요로 하는 집계 = 전부 슬라이딩 윈도우 스트림 집계:
- 이 계정, 최근 10분 결제 시도 횟수
- 이 IP, 최근 1시간 계정 수
- 이 카드, 오늘 누적 결제액

## 차별화 공식 (S급 레포 헌팅에서 배운 것)

1. 장애 서사: 기능 나열 대신 "Kafka 끊기면? 스키마 바뀌는 중에 이벤트 오면?" 시나리오 중심 README
2. 정직한 비교: Kafka Streams 기본 기능/Materialize/RisingWave 대비 뭐가 다른지 명시
3. 재현 가능한 데모
4. 검증 수치화: 정합성 검증 테스트 규모, 처리량 벤치

## 실행

```bash
# 전체 스택 기동 (postgres + kafka + connect + engine)
docker compose up -d --build

# 상태 확인
docker compose ps
curl -f http://localhost:8080/actuator/health
```

로컬 빌드/테스트 (Java 21 필요):

```bash
./gradlew :engine:test
```

## 참고

- 레포: harshithrao07/distributed-scheduler (3★) - 장애복구 중심 서사의 모범 (Spring+Kafka+Redis 구성)
- 논문: NSDI'26 EROICA (차등 관측성) - 파이프라인 장애 진단 관점 참고

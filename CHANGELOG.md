# 변경 이력

이 프로젝트의 주요 변경 사항을 기록합니다. 형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/)를 따릅니다.

## 2026-11-18
### 문서
- README를 아키텍처와 흐름 다이어그램 중심으로 다시 썼습니다.
- 요구사항 명세(SRS, ISO/IEC/IEEE 29148)와 설계 명세(SDS, IEEE 1016)를 추가하고, 피처 조회 API와 count_distinct 내용을 반영했습니다.

## 2026-11-02
### 추가
- 피처 조회 API `POST /api/v1/features/query`를 추가했습니다. `ticket-fraud-engine` 계약에 맞춘 응답이며, 제공할 수 없는 피처는 `missing`으로 돌려줍니다.
- `count_distinct` 집계를 추가했습니다. 값별 행 수를 보관해 UPDATE와 DELETE에도 정확합니다.
- 뷰 YAML에 `features` 선언과 부팅 시 검증을 추가했고, 기간 표기 `1d`를 지원합니다.
- 판정 엔진용 예시 뷰(`examples/ticket-fraud-views/`)를 추가했습니다.

## 2026-07-13
### 수정
- 재구축 도중 엔진이 죽으면 진행 상태가 사라져 재시작 후 IDLE로 보이던 문제를 고쳤습니다. 상태를 PostgreSQL에 저장하고, 중단된 재구축은 `ABORTED`로 표시합니다.
- 크래시 복구 테스트가 간헐적으로 실패하던 문제를 고쳤습니다.

## 2026-07-08
### 변경
- Debezium 폴링 간격을 500ms에서 10ms로, 배치 크기를 2048에서 256으로 낮춰 지연을 줄였습니다. 1,000/s에서 p95가 1,074ms에서 179ms로 줄었습니다.
- CDC 소스 토픽 파티션과 스트림 스레드를 1개에서 8개로 늘렸습니다. 10,000/s 10분 지속에서 p95 537ms, lag bounded를 달성했습니다.
- 벤치 하네스의 percentile 계산과 워밍업 제외 방식을 고쳤습니다.
- CI 추가
### 문서
- README에 벤치 실측 결과와 Materialize/RisingWave 대비 비교를 추가했습니다.

## 2026-07-07
### 추가
- Gradle 멀티 모듈(`engine`, `simulator`) 골격과 docker compose 인프라(PostgreSQL, Kafka KRaft, Debezium Connect)를 추가했습니다.
- YAML 뷰 정의 로딩과 검증을 추가했습니다. 잘못된 정의는 파일명과 필드 경로로 부팅을 중단합니다.
- 부팅 시 Debezium 커넥터를 자동 등록하고, 대상 테이블의 REPLICA IDENTITY FULL을 먼저 검사합니다.
- Kafka Streams로 조인/집계 뷰를 구체화합니다. count/sum/min/max/avg와 tumbling/sliding 윈도우를 지원합니다.
- REST 조회 API를 추가했습니다. 키 조회, 윈도우 범위 조회, ETag와 freshness 메타데이터를 제공합니다.
- 순서가 뒤집힌 이벤트를 LSN 기준으로 걸러내고, grace를 넘긴 지각 이벤트는 DLQ 토픽으로 보냅니다.
- exactly-once 처리(EOS v2 + LSN 멱등 필터)와 kill -9 재시작 검증 스크립트를 추가했습니다.
- 소스 스냅샷과 뷰를 비교하는 정합성 검증 API `POST /admin/verify/{view}`를 추가했습니다.
- 스키마 변경 중 무중단 뷰 재구축 API `POST /admin/views/{view}/rebuild`를 추가했습니다.
- 커머스 시뮬레이터와 원커맨드 데모, 벤치마크 하네스(`bench/run-bench.sh`)를 추가했습니다.
### 수정
- CDC 토픽이 없을 때 Streams가 영구 ERROR로 멈추던 기동 순서 문제를 고쳤습니다. 기동 전에 토픽을 먼저 만듭니다.
- 빈 `SIM_SEED` 값에서 시뮬레이터가 죽던 문제를 고쳤습니다.
- 시뮬레이터가 목표 rate의 75~78%만 내던 문제를 고쳤습니다.

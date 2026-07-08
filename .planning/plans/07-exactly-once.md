# Plan 07: exactly-once 보장

## 목표

이벤트 중복 전달과 엔진 비정상 종료/재시작에도 뷰 집계값이 각 커밋을 정확히 한 번 반영한 값이 되도록 한다. SPEC R6 (AC6-1 ~ AC6-4, 부정 기준 N1 포함) 을 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 04: Kafka Streams 토폴로지. Plan 06: LsnGuardProcessor (키별 최고 LSN 유지) 와 Debezium envelope 주입 테스트 유틸
- 보장 구조 (SPEC 그릴링 반영):
  - Kafka 내부 (consume-process-produce-commit): EOS v2 (`processing.guarantee=exactly_once_v2`)
  - Debezium → Kafka 구간은 at-least-once → 동일 LSN 재전달 가능. Plan 06의 LsnGuard는 "LSN 이하 폐기" 이므로 동일 LSN 중복도 자연 차단됨을 테스트로 고정 (경계: 같은 LSN = stale 취급 확인)
  - EOS v2에서 state store와 changelog가 트랜잭션에 묶이므로 kill -9 후 재시작 시 미커밋 처리분은 재처리되고 이중 반영 없음
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/ 설정 클래스 + scripts/chaos-restart-test.sh

## 작업

1. Streams 설정에 exactly_once_v2 적용 + 이를 검증하는 설정 테스트 (프로퍼티 조립 로직 대상)
2. 관련 파라미터 정리: commit.interval.ms (EOS 기본 100ms 유지 여부 - 지연 목표 R9 대비 주석), replication.factor (단일 브로커라 1), producer 멱등성
3. 동일 LSN 중복 주입 테스트: TopologyTestDriver로 같은 envelope 2회 주입 → sum/count 불변 확인
4. 재시작 카오스 스크립트 scripts/chaos-restart-test.sh:
   - compose 기동 → 고정 시드 데이터 N건을 PG에 커밋 (기대값 계산 가능한 결정적 데이터)
   - 주입 중간에 `docker kill` (SIGKILL) 로 엔진 강제 종료 → 재기동 → lag 0 대기
   - REST로 뷰 값 조회, 사전 계산된 기대값과 비교, 불일치 시 exit 1
5. 스크립트를 README 검증 섹션에 등록 (실행 방법 1줄)

## must-haves

- 기대값 비교가 "무중단 실행 결과와 동일" 을 실제로 검증 (성공 로그만 보고 통과 처리 금지)
- 카오스 스크립트는 반복 실행 가능 (실행마다 데이터 초기화 포함)
- EOS 끄면 실패하는 테스트가 있어야 보장이 반증 가능 (선택: at_least_once로 바꿔 중복 반영이 실제 발생함을 보이는 대조 테스트)

## 검증

- [ ] AC6-1: processing.guarantee=exactly_once_v2 활성이 설정 테스트로 검증된다
- [ ] AC6-2 (N1): 동일 LSN 이벤트 2회 주입에도 sum/count 값 불변 - 동일 커밋의 2회 반영이 발생하지 않는다
- [x] AC6-3: 처리 도중 kill -9 후 재시작 → 최종 뷰 값이 무중단 실행 기대값과 동일 (chaos-restart-test.sh 통과) - 2026-07-07 V2 실측: N=200 시드 후 처리 도중 `docker compose kill -s KILL engine` → 재기동 → 최종 뷰 count=200, sum=20100 (기대값 정확히 일치, 유실/중복 0). 클린 기동+kill -9 재시작 양 경로 한 실행에서 커버. 상세: .planning/v2-consistency-chaos-verification.md
- [x] AC6-4: scripts/chaos-restart-test.sh 가 저장소에 존재하고 반복 실행 가능하다 - 2026-07-07 V2 실측: 2회 연속 실행 (pass B, C) 전부 PASS. 매 실행 down -v로 초기화. 검증 중 스크립트 결함 3건 (한글 변수 파싱, lag 거짓양성, 시뮬레이터 시드 오염) 수정

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*ExactlyOnce*'
bash scripts/chaos-restart-test.sh
bash scripts/chaos-restart-test.sh   # 반복 실행 확인
```

# Plan 12 (V1.5): 기동 순서 결함 E1 수정 - CDC 토픽 선생성

## 배경 (V1 검증에서 발견, .planning/v1-demo-verification.md)

클린 compose 기동에서 StreamsManager(@Order 20)가 ConnectorRegistrar(@Order 10) 직후 즉시 Streams를 기동한다. 이 시점 소스 테이블은 비어 있고 (시뮬레이터는 engine healthy 이후 시작), Debezium은 빈 테이블 스냅샷에서 레코드를 내지 않아 CDC 토픽 (`cdcview.public.*`) 이 존재하지 않는다. 첫 리밸런스가 `MissingSourceTopicException` 으로 실패하고 기본 예외 핸들러가 SHUTDOWN_CLIENT를 택해 Streams가 영구 ERROR로 정지한다. 이후 토픽이 생겨도 복구되지 않는다. → AC10-4/AC10-5 블록.

## 방식 비교와 결정

| 방식 | 내용 | 판정 |
|---|---|---|
| A. 토픽 선생성 | Streams 기동 전 AdminClient로 CDC 토픽을 멱등 생성 | **채택** |
| B. 토픽 대기 | 토픽이 생길 때까지 폴링 후 기동 | 기각 |
| C. 예외 핸들러 재시도 | MissingSourceTopicException 시 REPLACE_THREAD | **보조로 채택** |

- B 기각 사유: 토픽은 첫 레코드가 있어야 생기므로 (Debezium/브로커 auto-create 는 produce 시점), 무트래픽 배포에서는 Streams 기동이 데이터 도착에 무기한 종속된다. 데모에서는 우연히 성립하지만 (시뮬레이터가 결국 데이터를 넣음) 비결정적이고, 타임아웃을 두면 그 안에 트래픽이 없을 때 다시 E1이다.
- A의 Debezium 정합성 검토:
  - 네이밍: Debezium PG 토픽명은 `{topic.prefix}.{schema}.{table}`. 엔진은 이미 `ConnectProperties.topicFor("public." + table)` 로 동일 규칙을 사용 중 (TopologyBuilder가 구독하는 이름과 같은 소스에서 파생) → 하드코딩 중복 없음, 정합.
  - 파티션/복제: 선생성 파티션 1, RF 1. 브로커 auto-create 기본 (`num.partitions=1`) 및 StreamsConfigFactory의 `replication.factor=1` (단일 브로커 데모 전제) 과 동일 → Debezium이 auto-create 했을 결과와 같은 형상이라 회귀 없음. 조인 co-partitioning도 파티션 1로 유지.
  - Debezium은 이미 존재하는 토픽에 그대로 produce 한다 (토픽 존재는 커넥터에 무해).
- C 채택 사유 (보조): 선생성 이후에도 메타데이터 전파 지연 등 잔여 경합으로 리밸런스가 같은 예외를 낼 수 있다. MissingSourceTopicException (cause 체인 포함) 에 한해 REPLACE_THREAD로 재시도하고, 그 외 예외는 기존 기본 동작 (SHUTDOWN_CLIENT) 을 유지해 실제 결함을 은폐하지 않는다.

## 작업

1. `cdc/CdcTopicProvisioner` (@Order 15, ApplicationRunner): ViewRegistry 소스 테이블 → `topicFor("public."+t)` 로 토픽명 파생, AdminClient `describeTopics` 로 부재 확인 후 `createTopics` (파티션 1, RF 1). `TopicExistsException` 은 성공 취급 (멱등). 호출 타임아웃 30초, 그 외 실패는 예외 전파로 부팅 중단 (fail-fast, AC1-2 스타일). `cdcview.bootstrap.streams=false` 면 생략 (단위 테스트 컨텍스트 보호).
2. `topology/StreamsManager`: `setUncaughtExceptionHandler` 추가. cause 체인에 MissingSourceTopicException 이 있으면 REPLACE_THREAD (경고 로그), 아니면 SHUTDOWN_CLIENT. 판정 로직은 정적 메서드로 분리해 단위 테스트.
3. 단위 테스트: 토픽명 파생/멱등 처리 (Admin 목), 예외 핸들러 판정 (직접/래핑/무관 예외).
4. compose 클린 기동 (볼륨 포함 제거 후) 으로 AC10-4/AC10-5 재실측, plans/10 체크박스 반영.

## 셀프 그릴링 기록

| 공격 | 반영 |
|---|---|
| 빈 테이블 스냅샷이면 선생성해도 데이터가 없다 - 뷰가 빈 값을 주지 않나 | Streams는 빈 토픽 구독으로 RUNNING 도달, 데이터 도착 시 처리. 뷰 API는 키 부재 시 404 (기존 AC4-3) 로 일관 |
| 브로커 접근 불가 시 무한 대기 | AdminClient 호출 타임아웃 30초 + 실패 시 부팅 중단. compose는 kafka healthy 이후 엔진을 시작하므로 정상 경로에서는 도달 안 함 |
| 커넥터 등록 실패와의 관계 | 선생성은 Connect와 무관 (브로커 직접). 커넥터 등록 실패는 기존대로 ConnectorRegistrar에서 예외 전파 |
| REPLACE_THREAD 무한 루프 위험 | MissingSourceTopicException 한정. 선생성이 주 방어라 이 경로는 잔여 경합 전용. 그 외 예외는 SHUTDOWN_CLIENT 유지 |
| 재시도 중 health 표시 | actuator health는 기존대로 Streams 상태 미포함 (V1과 동일 동작, 범위 외). 뷰 API는 REBALANCING 중 StreamsNotReady 응답 - 기존 동작 유지 |
| 섀도 재구축 (Plan 09) 경로 회귀 | 섀도는 토픽이 이미 존재하는 시점에만 기동되므로 영향 없음. 선생성은 메인 기동 전 1회 |
| 파티션 수를 늘리면? | 범위 외 (단일 인스턴스/단일 브로커 전제, SPEC 경계). 파티션 변경은 co-partitioning 재설계 필요 - 멀티 인스턴스 plan의 몫 |

## 검증 (수용 기준)

- [x] AC12-1 (= AC10-4 재실측): 볼륨까지 지운 클린 상태에서 compose up 후 5분 내 수동 개입 없이 GET /views/seller-dashboard/{id} 가 실데이터를 반환한다 - 2026-07-07 실측: demo-check.sh PASS, compose up 시작부터 실데이터까지 52초 (이미지 재빌드 포함). 엔진 로그에 토픽 3개 선생성 후 REBALANCING → RUNNING, MissingSourceTopicException 0건
- [x] AC12-2 (= AC10-5): README 데모 절차 그대로 재현 가능하다 - 2026-07-07 실측: 8080 그대로 사용 가능 상태에서 README 명령어 그대로 통과 (이번 실측에서는 포트 충돌 없어 override 불필요. 충돌 시 우회 절차는 v1.5 리포트에 기록)
- [x] AC12-3 (부정 기준): 토픽이 이미 존재하는 상태의 엔진 재시작 경로가 회귀하지 않는다 - 2026-07-07 실측: restart 후 +47초에 서빙 재개 (EOS 상태 복원 + 리밸런스), order_count 11 → 32 → 44로 신규 이벤트 계속 반영, MissingSourceTopicException 0건
- [x] AC12-4: 단위 테스트 - CdcTopicProvisionerTest 5건 (이름 파생/부재만 생성/전부 존재 시 미생성/TopicExists 경합 멱등/그 외 실패 전파) + StreamsExceptionPolicyTest 3건 (직접/래핑 → REPLACE_THREAD, 그 외 → SHUTDOWN_CLIENT) 전부 통과. 전체 :engine:test 58건 중 57건 통과, 1건 (ReplicaIdentityCheckerIT) 은 testcontainers docker-java API 버전 협상 환경 문제로 실패 - DOCKER_API_VERSION=1.44 지정 시 통과 (코드 무관)

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test
docker compose down -v && docker compose up -d --build   # 클린 기동
bash scripts/demo-check.sh
```

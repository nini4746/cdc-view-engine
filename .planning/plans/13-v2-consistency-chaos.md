# Plan 13 (V2): 정합성·카오스 런타임 검증

## 목표

V1/V1.5에서 검증한 데모 헬스 위에서, exactly-once 카오스 재시작 (AC6-3, AC6-4) 과 10만 이벤트 정합성 (AC7-2) 을 실제 compose 스택에서 실측 확정한다. V2는 검증 plan이다 - 엔진 코드 결함 발견 시 수정하지 않고 목록화한다 (데모 글루/스크립트 결함은 수정).

## 판정 기준과 측정 방법

| 항목 | 판정 기준 | 측정 방법 |
|---|---|---|
| AC6-3 | kill -9 후 재시작해도 최종 뷰 값 = 무중단 기대값 (유실/중복 0) | scripts/chaos-restart-test.sh: 결정적 시드 N=200건 (amount=1..N, 기대 count=N, sum=N(N+1)/2) 커밋 → 처리 도중 `docker compose kill -s KILL engine` → 재기동 → consumer lag 0 대기 → 뷰 count/sum 기대값 일치 확인 |
| AC6-4 | 위 시나리오가 저장소 스크립트로 반복 실행 가능 | 동일 스크립트 최소 2회 연속 실행 전부 PASS (스크립트가 매 실행 down -v로 초기화) |
| AC7-2 | 10만 이벤트 처리 후 quiesce 조건에서 /admin/verify 불일치 0건 (verdict pass) | 시뮬레이터로 누적 10만+ 이벤트 주입 (달성 rate 로그로 확인) → 시뮬레이터 정지 → 60초+ 대기 (lag 0) → `POST /admin/verify/seller-dashboard` 및 `POST /admin/verify/seller-sales-window` → quiesced=true, mismatchCount=0, verdict=pass |

카오스 경로 커버리지: chaos-restart-test.sh 는 매 실행 `down -v` → `up --build` 로 시작하므로 **클린 기동 경로** (Plan 12 토픽 선생성 포함) 와 **kill -9 재시작 경로** 를 한 실행에서 모두 지난다.

## 사전 조사 결정 기록

### O1: 시뮬레이터 achieved 75~78/100 (V1 관찰) - 시뮬레이터 결함으로 판정, 수정

- 원인: `Simulator.run()` 이 10ms 창마다 `perWindow` 건을 쓰고 `windowMs - elapsed` 만큼 상대 sleep 한다. DB 왕복 (~1-3ms) + `Thread.sleep` 오버슬립 (OS 타이머 해상도) 이 매 창마다 누적되는데 보정이 없어 실제 주기가 ~13ms 로 늘어난다 (10/13 ≈ 0.77 → 실측 75~78% 와 일치). 토큰 버킷 (Plan 10 명세) 이 아니라 드리프트가 그대로 rate 손실로 이어지는 구조.
- 수정: 경과 시간 기준 deficit 방식으로 교체 - 매 루프에서 `expected = elapsedMs * rate / 1000` 과 `emitted` 의 차이만큼 배치로 커밋. 오버슬립이 다음 루프에서 자동 보정되어 장기 평균이 target 에 수렴한다. 일시 정지 후 폭주 방지로 배치 상한 (1초 분량) 을 둔다.
- 10만 이벤트 시간 영향: 수정 후 target 달성 전제로 SIM_RATE=300 사용 → 10만 이벤트 약 5.6분 (미수정 75/s 였다면 약 22분).

### 스크립트 결함 (수정)

- chaos-restart-test.sh: compose up 시 시뮬레이터가 함께 떠서 seller 1 에 랜덤 이벤트를 계속 넣으므로 결정적 기대값이 오염된다 → 시드 전 `docker compose stop simulator` 추가. REST 주소 8080 하드코딩 → 환경변수 오버라이드 허용.
- demo-check.sh (O2): REST 주소 8080 하드코딩 → `REST="${REST:-http://localhost:8080}"` 로 환경변수화.

## 검증 (수용 기준 - SPEC 체크박스에 대응)

- [x] AC6-3: chaos-restart-test.sh 실행 - kill -9 후 재시작한 최종 뷰 값이 기대값 (count=200, sum=20100) 과 동일. 실측 count=200, sum=20100.0 일치
- [x] AC6-4: 동일 스크립트 2회 연속 실행 전부 PASS (반복 실행 가능성 실증). pass B, C 모두 exit 0
- [x] AC7-2: 10만+ 이벤트 처리 후 quiesce 조건에서 seller-dashboard 검증 불일치 0건, verdict pass (102,655 이벤트, mismatch 0/100). seller-sales-window는 엔진이 MVP 범위 밖으로 명시 거부 - AC7-2는 조인 뷰로 충족
- [x] 보조: 수정된 시뮬레이터 achieved rate 가 target 에 수렴 (O1 해소). target=300 achieved=300 지속 확인 (일시 stall 후 maxBatch 상한 내 catch-up 버스트 존재하나 장기 평균 수렴)

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
bash scripts/chaos-restart-test.sh        # 2회 실행
SIM_RATE=300 docker compose up -d simulator
# total >= 100000 확인 후:
docker compose stop simulator && sleep 60
curl -X POST http://localhost:8080/admin/verify/seller-dashboard | jq
curl -X POST http://localhost:8080/admin/verify/seller-sales-window | jq
```

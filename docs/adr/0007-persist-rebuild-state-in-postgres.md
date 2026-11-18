# 0007. 재구축 상태를 소스 PostgreSQL에 영속화

- 상태: 채택
- 날짜: 2026-07-13

## 맥락
0005의 메모리 보관 방식은 재구축 도중 크래시하면 상태를 잃었습니다.
재시작 후 상태 조회가 IDLE을 반환해, 중단된 재구축이 있었는지 알 수 없었습니다.

## 결정
- `JdbcRebuildStateStore`가 기존 소스 PG DataSource의 `cdcview_rebuild_state` 테이블에 상태를 저장합니다.
- `RebuildService`는 모든 상태 전이를 메모리 맵과 store에 함께 기록합니다.
- 메모리에 항목이 없는데 store에 비종결 레코드가 있으면 크래시로 중단된 것으로 보고 새 종결 상태 `ABORTED`로 바꿉니다.
- 섀도 빌드 재개는 여전히 범위 밖입니다(0004). 구버전이 계속 서빙되고, `ABORTED` 이후 바로 재구축을 다시 요청할 수 있습니다.

## 대안
- 새 저장소 도입: `ColumnChecker`, `SqlExpectedViewProvider`가 이미 쓰는 JDBC 인프라를 재사용하는 편이 단순합니다.

## 결과
- 장점: 새 인프라 없이 재시작 후 크래시를 감지합니다. 중단 사실이 조용히 사라지지 않습니다.
- 단점/감수한 점: 엔진이 소스 DB에 자체 테이블 하나를 둡니다(`docker/postgres/init.sql`).

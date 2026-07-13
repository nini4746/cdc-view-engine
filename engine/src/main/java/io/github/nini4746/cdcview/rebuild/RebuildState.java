package io.github.nini4746.cdcview.rebuild;

/**
 * 재구축 진행 상태.
 *
 * <p>메모리(단일 프로세스 실행 중 빠른 조회/갱신)와 {@link RebuildStateStore}(재시작 생존)에 이중으로
 * 유지된다. 프로세스 재시작 후 메모리에 없던 비종결 상태 레코드를 발견하면 크래시로 중단된 것으로 보고
 * {@link Status#ABORTED}로 전환한다(섀도 스트림 인스턴스 자체는 프로세스와 함께 소멸하므로 재개는
 * 지원하지 않음 - 이전 버전은 계속 서빙되며, 재구축은 처음부터 다시 요청하면 된다).
 */
public record RebuildState(
        String view,
        Status status,
        int fromVersion,
        int toVersion,
        String message,
        long startedAt,
        long updatedAt) {

    public enum Status {
        IDLE, VALIDATING, SNAPSHOTTING, CATCHING_UP, SWAPPED, FAILED, ABORTED
    }

    public RebuildState withStatus(Status s, String msg) {
        return new RebuildState(view, s, fromVersion, toVersion, msg, startedAt, System.currentTimeMillis());
    }

    public boolean isTerminal() {
        return status == Status.SWAPPED || status == Status.FAILED || status == Status.ABORTED;
    }
}

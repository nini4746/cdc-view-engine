package io.github.nini4746.cdcview.rebuild;

/**
 * 재구축 진행 상태. 인메모리 관리(재시작 시 미완의 섀도는 버리고 구버전 서빙 유지, 재개는 범위 밖).
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
        IDLE, VALIDATING, SNAPSHOTTING, CATCHING_UP, SWAPPED, FAILED
    }

    public RebuildState withStatus(Status s, String msg) {
        return new RebuildState(view, s, fromVersion, toVersion, msg, startedAt, System.currentTimeMillis());
    }

    public boolean isTerminal() {
        return status == Status.SWAPPED || status == Status.FAILED;
    }
}

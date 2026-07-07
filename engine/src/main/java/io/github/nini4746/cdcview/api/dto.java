package io.github.nini4746.cdcview.api;

import java.util.List;
import java.util.Map;

/** REST 응답 DTO 모음. */
final class dto {
    private dto() {
    }
}

/**
 * freshness 메타.
 *
 * @param sourceCommitTs 마지막으로 반영된 이벤트의 PG 커밋 시각(source.ts_ms)
 * @param appliedAt      뷰에 반영된 시각(엔진 wall clock)
 */
record Freshness(long sourceCommitTs, long appliedAt) {
}

/** 비윈도우 뷰 키 조회 응답. */
record ViewResponse(String view, String key, Map<String, Object> values,
                    Freshness freshness, int version) {
}

/** 윈도우 하나의 결과. window는 [start, end) 반개구간. */
record WindowEntry(long start, long end, Map<String, Object> values, Freshness freshness) {
}

/** 윈도우 뷰 범위 조회 응답. */
record WindowResponse(String view, String key, List<WindowEntry> windows) {
}

/** 404/400 등 에러 바디. */
record ErrorResponse(String error, String view, String key) {
}

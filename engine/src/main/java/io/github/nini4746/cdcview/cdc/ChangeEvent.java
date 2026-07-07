package io.github.nini4746.cdcview.cdc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;

/**
 * Debezium 변경 이벤트의 엔진 내부 표현.
 *
 * @param table  소스 테이블명 (스키마 제외)
 * @param op     연산 종류
 * @param before UPDATE/DELETE의 이전 이미지 (REPLICA IDENTITY FULL 전제). 없으면 null
 * @param after  INSERT/UPDATE/스냅샷의 이후 이미지. DELETE면 null
 * @param lsn    PG LSN. 같은 키에서 순서 기준(작을수록 과거)
 * @param tsMs   커밋 시각(source.ts_ms). event time 및 지연 측정 기준
 */
public record ChangeEvent(
        String table,
        Op op,
        Map<String, Object> before,
        Map<String, Object> after,
        long lsn,
        long tsMs) {

    public enum Op {
        CREATE, UPDATE, DELETE, READ;

        static Op fromCode(String code) {
            return switch (code) {
                case "c" -> CREATE;
                case "u" -> UPDATE;
                case "d" -> DELETE;
                case "r" -> READ;
                default -> throw new IllegalArgumentException("알 수 없는 op 코드: " + code);
            };
        }
    }

    @JsonIgnore
    public boolean isDelete() {
        return op == Op.DELETE;
    }

    /** 집계에 반영할 최신 이미지: DELETE면 before, 그 외 after. */
    @JsonIgnore
    public Map<String, Object> effectiveImage() {
        return op == Op.DELETE ? before : after;
    }
}

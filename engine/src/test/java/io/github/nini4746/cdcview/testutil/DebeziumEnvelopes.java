package io.github.nini4746.cdcview.testutil;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 테스트용 Debezium envelope(JSON, schemas.enable=false) 생성기.
 * Debezium을 거치지 않고 CDC 토픽에 임의 순서로 이벤트를 주입할 때 사용한다(Plan 04/06/07/11 재사용).
 */
public final class DebeziumEnvelopes {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DebeziumEnvelopes() {
    }

    public static String insert(String table, long lsn, long tsMs, Map<String, Object> after) {
        return envelope(table, "c", lsn, tsMs, null, after);
    }

    public static String read(String table, long lsn, long tsMs, Map<String, Object> after) {
        return envelope(table, "r", lsn, tsMs, null, after);
    }

    public static String update(String table, long lsn, long tsMs,
                                Map<String, Object> before, Map<String, Object> after) {
        return envelope(table, "u", lsn, tsMs, before, after);
    }

    public static String delete(String table, long lsn, long tsMs, Map<String, Object> before) {
        return envelope(table, "d", lsn, tsMs, before, null);
    }

    public static String envelope(String table, String op, long lsn, long tsMs,
                                  Map<String, Object> before, Map<String, Object> after) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("table", table);
        source.put("lsn", lsn);
        source.put("ts_ms", tsMs);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("before", before);
        payload.put("after", after);
        payload.put("source", source);
        payload.put("op", op);
        payload.put("ts_ms", tsMs);
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** 컬럼 맵을 빠르게 만드는 헬퍼. row("id",1L,"seller_id",10L,"amount",100.0). */
    public static Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }
}

package io.github.nini4746.cdcview.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Debezium JSON envelope를 {@link ChangeEvent}로 파싱한다.
 *
 * <p>커넥터는 value.converter.schemas.enable=false로 설정하므로 값은 스키마 래퍼 없는 payload다.
 * 안전을 위해 "payload" 래퍼가 있으면 자동 언랩한다. DELETE tombstone(null 값)은 호출 측에서 무시한다.
 */
public class ChangeEventParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public ChangeEvent parse(byte[] value) {
        if (value == null) {
            return null; // tombstone
        }
        try {
            JsonNode root = mapper.readTree(value);
            if (root.has("payload") && root.get("payload").isObject()) {
                root = root.get("payload");
            }
            if (root.isNull() || !root.has("op")) {
                return null;
            }
            ChangeEvent.Op op = ChangeEvent.Op.fromCode(root.get("op").asText());
            JsonNode source = root.get("source");
            if (source == null) {
                throw new IllegalArgumentException("Debezium envelope에 source가 없습니다");
            }
            String table = source.path("table").asText();
            long lsn = source.path("lsn").asLong(0L);
            long tsMs = source.path("ts_ms").asLong(0L);
            Map<String, Object> before = toMap(root.get("before"));
            Map<String, Object> after = toMap(root.get("after"));
            return new ChangeEvent(table, op, before, after, lsn, tsMs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Debezium envelope 파싱 실패", e);
        }
    }

    private Map<String, Object> toMap(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        Map<String, Object> map = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            map.put(e.getKey(), scalar(e.getValue()));
        }
        return map;
    }

    private Object scalar(JsonNode v) {
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            // decimal.handling.mode=double 이므로 금액도 double로 도착
            return v.isIntegralNumber() ? (Object) v.asLong() : (Object) v.asDouble();
        }
        if (v.isBoolean()) {
            return v.asBoolean();
        }
        return v.asText();
    }
}

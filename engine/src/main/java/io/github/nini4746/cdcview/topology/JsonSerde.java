package io.github.nini4746.cdcview.topology;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Jackson 기반 범용 JSON Serde. 리파티션/상태 저장 값 직렬화에 사용. */
public class JsonSerde<T> implements Serde<T> {

    private final Class<T> type;
    private final ObjectMapper mapper;

    public JsonSerde(Class<T> type) {
        this.type = type;
        // record(ChangeEvent)는 canonical 생성자로, POJO(AggregateState)는 게터/세터로 처리.
        // 파생 게터 등 예기치 못한 필드는 무시한다.
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("JSON 직렬화 실패: " + type.getSimpleName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return mapper.readValue(bytes, type);
            } catch (Exception e) {
                throw new RuntimeException("JSON 역직렬화 실패: " + type.getSimpleName(), e);
            }
        };
    }
}

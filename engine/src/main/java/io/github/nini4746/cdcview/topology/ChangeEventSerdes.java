package io.github.nini4746.cdcview.topology;

import io.github.nini4746.cdcview.cdc.ChangeEvent;
import io.github.nini4746.cdcview.cdc.ChangeEventDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/** ChangeEvent용 Serde 모음. */
public final class ChangeEventSerdes {

    private ChangeEventSerdes() {
    }

    /**
     * 소스 CDC 토픽 소비용: Debezium envelope를 파싱하는 디시리얼라이저 +
     * (리파티션 대비) JSON 시리얼라이저.
     */
    public static Serde<ChangeEvent> forSourceTopic() {
        JsonSerde<ChangeEvent> json = new JsonSerde<>(ChangeEvent.class);
        return Serdes.serdeFrom(json.serializer(), new ChangeEventDeserializer());
    }

    /** 리파티션/그룹핑용: 순수 JSON 왕복. */
    public static Serde<ChangeEvent> json() {
        return new JsonSerde<>(ChangeEvent.class);
    }
}

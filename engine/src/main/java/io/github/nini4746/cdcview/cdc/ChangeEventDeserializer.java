package io.github.nini4746.cdcview.cdc;

import org.apache.kafka.common.serialization.Deserializer;

/** Kafka 토폴로지에서 CDC 토픽 값을 {@link ChangeEvent}로 역직렬화한다. */
public class ChangeEventDeserializer implements Deserializer<ChangeEvent> {

    private final ChangeEventParser parser = new ChangeEventParser();

    @Override
    public ChangeEvent deserialize(String topic, byte[] data) {
        return parser.parse(data);
    }
}

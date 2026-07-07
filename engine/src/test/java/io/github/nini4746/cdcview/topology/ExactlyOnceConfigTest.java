package io.github.nini4746.cdcview.topology;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.config.KafkaProperties;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

/** AC6-1: processing.guarantee=exactly_once_v2 활성 검증. */
class ExactlyOnceConfigTest {

    @Test
    void defaultsToExactlyOnceV2() {
        StreamsConfigFactory factory = new StreamsConfigFactory(new KafkaProperties());
        Properties p = factory.create("cdc-view-engine");
        assertThat(p.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))
                .isEqualTo(StreamsConfig.EXACTLY_ONCE_V2);
        // 단일 브로커 데모: 복제 계수 1
        assertThat(p.get(StreamsConfig.REPLICATION_FACTOR_CONFIG)).isEqualTo(1);
    }

    @Test
    void guaranteeIsConfigurable() {
        StreamsConfigFactory factory = new StreamsConfigFactory(new KafkaProperties());
        factory.setProcessingGuarantee(StreamsConfig.AT_LEAST_ONCE);
        assertThat(factory.create("x").get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))
                .isEqualTo(StreamsConfig.AT_LEAST_ONCE);
    }
}

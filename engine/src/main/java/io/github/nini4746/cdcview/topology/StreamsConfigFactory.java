package io.github.nini4746.cdcview.topology;

import io.github.nini4746.cdcview.config.KafkaProperties;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Component;

/**
 * Kafka Streams 설정 조립. processing.guarantee는 Plan 07에서 exactly_once_v2로 설정된다.
 */
@Component
public class StreamsConfigFactory {

    private final KafkaProperties kafka;

    // Plan 07에서 exactly_once_v2로 설정. 기본은 at_least_once.
    private String processingGuarantee = StreamsConfig.EXACTLY_ONCE_V2;

    public StreamsConfigFactory(KafkaProperties kafka) {
        this.kafka = kafka;
    }

    public Properties create(String applicationId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(StreamsConfig.STATE_DIR_CONFIG, kafka.getStateDir());
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        // 단일 브로커 데모: 복제 계수 1
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        // EOS 기본 commit.interval.ms=100ms 유지(지연 목표 R9 대비 균형). 필요 시 벤치에서 튜닝.
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        // Debezium 초기 스냅샷(op=r) 포함 전체 히스토리를 읽어 상태를 구축(재구축 섀도도 동일)
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        return p;
    }

    public String getProcessingGuarantee() {
        return processingGuarantee;
    }

    public void setProcessingGuarantee(String processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
    }
}

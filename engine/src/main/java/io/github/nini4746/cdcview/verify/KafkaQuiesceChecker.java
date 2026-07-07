package io.github.nini4746.cdcview.verify;

import io.github.nini4746.cdcview.config.KafkaProperties;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * AdminClient로 Streams consumer group의 committed offset 대비 로그 끝 오프셋 차이를 합산해 lag을 구한다.
 */
@Component
public class KafkaQuiesceChecker implements QuiesceChecker {

    private static final Logger log = LoggerFactory.getLogger(KafkaQuiesceChecker.class);

    private final KafkaProperties kafka;

    public KafkaQuiesceChecker(KafkaProperties kafka) {
        this.kafka = kafka;
    }

    @Override
    public long totalLag() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            ListConsumerGroupOffsetsResult offsets =
                    admin.listConsumerGroupOffsets(kafka.getApplicationId());
            Map<TopicPartition, OffsetAndMetadata> committed =
                    offsets.partitionsToOffsetAndMetadata().get();
            if (committed.isEmpty()) {
                return 0;
            }
            Map<TopicPartition, OffsetSpec> latestSpec = new java.util.HashMap<>();
            committed.keySet().forEach(tp -> latestSpec.put(tp, OffsetSpec.latest()));
            var endOffsets = admin.listOffsets(latestSpec).all().get();

            long lag = 0;
            for (var e : committed.entrySet()) {
                long end = endOffsets.get(e.getKey()).offset();
                long pos = e.getValue().offset();
                lag += Math.max(0, end - pos);
            }
            return lag;
        } catch (Exception e) {
            log.warn("lag 조회 실패, non-quiesced로 처리: {}", e.getMessage());
            return Long.MAX_VALUE;
        }
    }
}

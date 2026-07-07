package io.github.nini4746.cdcview.rebuild;

import io.github.nini4746.cdcview.api.InstanceDirectory;
import io.github.nini4746.cdcview.config.KafkaProperties;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.topology.StreamsConfigFactory;
import io.github.nini4746.cdcview.topology.TopologyBuilder;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Kafka 기반 섀도 빌더. 새 application.id의 KafkaStreams를 earliest부터 기동해
 * 새 버전 store(view-{name}-v{N})를 전체 히스토리로부터 재구축한다.
 * 따라잡기는 섀도 consumer group lag 0으로 판정한다. 실행에는 Kafka가 필요하다(Docker).
 */
@Component
public class KafkaShadowBuilder implements ShadowBuilder {

    private static final Logger log = LoggerFactory.getLogger(KafkaShadowBuilder.class);

    private final TopologyBuilder topologyBuilder;
    private final StreamsConfigFactory configFactory;
    private final KafkaProperties kafka;
    private final InstanceDirectory directory;

    private final ConcurrentMap<String, KafkaStreams> shadows = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> appIds = new ConcurrentHashMap<>();

    public KafkaShadowBuilder(TopologyBuilder topologyBuilder, StreamsConfigFactory configFactory,
                              KafkaProperties kafka, InstanceDirectory directory) {
        this.topologyBuilder = topologyBuilder;
        this.configFactory = configFactory;
        this.kafka = kafka;
        this.directory = directory;
    }

    private String tag(String view, int version) {
        return view + "-v" + version;
    }

    @Override
    public void startShadow(ViewDefinition def, int newVersion) {
        String view = def.getName();
        String storeName = "view-" + view + "-v" + newVersion;
        String appId = kafka.getApplicationId() + "-shadow-" + tag(view, newVersion);

        StreamsBuilder sb = new StreamsBuilder();
        topologyBuilder.buildView(sb, def, storeName);
        Properties props = configFactory.create(appId);
        // 섀도 상태는 별도 디렉토리에 격리
        props.put(StreamsConfig.STATE_DIR_CONFIG, kafka.getStateDir() + "/shadow-" + tag(view, newVersion));

        KafkaStreams shadow = new KafkaStreams(sb.build(), props);
        shadow.start();
        shadows.put(tag(view, newVersion), shadow);
        appIds.put(tag(view, newVersion), appId);
        directory.register(shadow);
        log.info("섀도 스트림 기동: view={} v{} appId={}", view, newVersion, appId);
    }

    @Override
    public boolean caughtUp(String view, int newVersion) {
        KafkaStreams shadow = shadows.get(tag(view, newVersion));
        if (shadow == null || !shadow.state().isRunningOrRebalancing()) {
            return false;
        }
        return lag(appIds.get(tag(view, newVersion))) == 0;
    }

    @Override
    public void promote(String view, int newVersion) {
        // 이미 디렉토리에 등록되어 있으므로 스왑 후 조회가 새 버전 store를 찾는다. 추가 작업 없음.
        log.info("섀도 승격: view={} v{}", view, newVersion);
    }

    @Override
    public void discard(String view, int version) {
        KafkaStreams shadow = shadows.remove(tag(view, version));
        appIds.remove(tag(view, version));
        if (shadow != null) {
            directory.unregister(shadow);
            shadow.close(Duration.ofSeconds(10));
            log.info("섀도 정리: view={} v{}", view, version);
        }
        // 구버전이 메인 인스턴스에 있으면 개별 제거는 불가(메인은 전체 뷰 토폴로지). MVP 한계.
    }

    private long lag(String groupId) {
        if (groupId == null) {
            return Long.MAX_VALUE;
        }
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            if (committed.isEmpty()) {
                return Long.MAX_VALUE; // 아직 커밋 전 → 미완
            }
            Map<TopicPartition, OffsetSpec> spec = new HashMap<>();
            committed.keySet().forEach(tp -> spec.put(tp, OffsetSpec.latest()));
            var end = admin.listOffsets(spec).all().get();
            long lag = 0;
            for (var e : committed.entrySet()) {
                lag += Math.max(0, end.get(e.getKey()).offset() - e.getValue().offset());
            }
            return lag;
        } catch (Exception e) {
            log.warn("섀도 lag 조회 실패: {}", e.getMessage());
            return Long.MAX_VALUE;
        }
    }
}

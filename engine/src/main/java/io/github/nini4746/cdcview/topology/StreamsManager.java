package io.github.nini4746.cdcview.topology;

import io.github.nini4746.cdcview.config.BootstrapProperties;
import io.github.nini4746.cdcview.config.KafkaProperties;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * 메인 Kafka Streams 인스턴스의 라이프사이클을 관리한다. 부팅 시 전체 뷰 토폴로지를 기동하고,
 * 종료 시 정상 종료한다. Plan 05 REST 조회는 여기 보관된 KafkaStreams에서 store를 얻는다.
 */
@Component
@Order(20)
public class StreamsManager implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StreamsManager.class);

    private final TopologyBuilder topologyBuilder;
    private final StreamsConfigFactory configFactory;
    private final KafkaProperties kafkaProps;
    private final BootstrapProperties bootstrap;

    private volatile KafkaStreams streams;

    public StreamsManager(TopologyBuilder topologyBuilder, StreamsConfigFactory configFactory,
                          KafkaProperties kafkaProps, BootstrapProperties bootstrap) {
        this.topologyBuilder = topologyBuilder;
        this.configFactory = configFactory;
        this.kafkaProps = kafkaProps;
        this.bootstrap = bootstrap;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!bootstrap.isStreams()) {
            log.info("cdcview.bootstrap.streams=false → 메인 스트림 기동 생략");
            return;
        }
        StreamsBuilder builder = new StreamsBuilder();
        topologyBuilder.build(builder);
        Topology topology = builder.build();
        log.info("메인 토폴로지 구성:\n{}", topology.describe());

        streams = new KafkaStreams(topology, configFactory.create(kafkaProps.getApplicationId()));
        streams.setStateListener((newState, oldState) ->
                log.info("Streams 상태 전이: {} → {}", oldState, newState));
        streams.start();
    }

    /** Plan 05/08 store 조회용. 미기동이면 null. */
    public KafkaStreams streams() {
        return streams;
    }

    public boolean isRunning() {
        return streams != null && streams.state().isRunningOrRebalancing();
    }

    @PreDestroy
    public void shutdown() {
        if (streams != null) {
            log.info("메인 스트림 종료");
            streams.close(Duration.ofSeconds(10));
        }
    }
}

package io.github.nini4746.cdcview.topology;

import io.github.nini4746.cdcview.api.InstanceDirectory;
import io.github.nini4746.cdcview.config.BootstrapProperties;
import io.github.nini4746.cdcview.config.KafkaProperties;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
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
    private final InstanceDirectory instanceDirectory;

    private volatile KafkaStreams streams;

    public StreamsManager(TopologyBuilder topologyBuilder, StreamsConfigFactory configFactory,
                          KafkaProperties kafkaProps, BootstrapProperties bootstrap,
                          InstanceDirectory instanceDirectory) {
        this.topologyBuilder = topologyBuilder;
        this.configFactory = configFactory;
        this.kafkaProps = kafkaProps;
        this.bootstrap = bootstrap;
        this.instanceDirectory = instanceDirectory;
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
        streams.setUncaughtExceptionHandler(ex -> {
            StreamThreadExceptionResponse response = responseFor(ex);
            if (response == StreamThreadExceptionResponse.REPLACE_THREAD) {
                log.warn("소스 토픽 부재로 스트림 스레드 교체 후 재시도 (토픽 선생성 이후 잔여 경합): {}",
                        ex.getMessage());
            } else {
                log.error("스트림 처리 예외 → 클라이언트 종료", ex);
            }
            return response;
        });
        streams.start();
        instanceDirectory.register(streams);
    }

    /**
     * Streams 예외 처리 방침 (Plan 12, E1). CDC 토픽은 기동 전 선생성되므로
     * MissingSourceTopicException은 메타데이터 전파 지연 등 일시 경합 → 스레드 교체로 재시도.
     * 그 외 예외는 기존 기본 동작(SHUTDOWN_CLIENT)을 유지해 결함을 은폐하지 않는다.
     */
    static StreamThreadExceptionResponse responseFor(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof MissingSourceTopicException) {
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
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

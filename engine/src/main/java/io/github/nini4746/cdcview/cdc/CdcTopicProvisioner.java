package io.github.nini4746.cdcview.cdc;

import io.github.nini4746.cdcview.config.BootstrapProperties;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.KafkaProperties;
import io.github.nini4746.cdcview.config.ViewRegistry;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Streams 기동 전에 CDC 소스 토픽을 멱등 선생성한다 (V1 결함 E1 수정, Plan 12).
 *
 * <p>Debezium은 첫 레코드를 쓸 때에야 토픽을 만들기 때문에, 빈 테이블로 시작하는 클린 기동에서는
 * Streams가 소스 토픽 부재(MissingSourceTopicException)로 영구 정지했다. 커넥터 등록(@Order 10)과
 * Streams 기동(@Order 20) 사이(@Order 15)에서 토픽을 미리 만들어 경합을 제거한다.
 *
 * <p>파티션 1 / RF 1은 브로커 auto-create 기본값 및 StreamsConfigFactory의 replication.factor=1
 * (단일 브로커 데모 전제)과 동일한 형상이라, Debezium이 auto-create 했을 결과와 차이가 없다.
 */
@Component
@Order(15)
public class CdcTopicProvisioner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CdcTopicProvisioner.class);

    static final short REPLICATION_FACTOR = 1;
    private static final long TIMEOUT_SECONDS = 30;

    private final ViewRegistry viewRegistry;
    private final ConnectProperties connect;
    private final KafkaProperties kafka;
    private final BootstrapProperties bootstrap;

    public CdcTopicProvisioner(ViewRegistry viewRegistry, ConnectProperties connect,
                               KafkaProperties kafka, BootstrapProperties bootstrap) {
        this.viewRegistry = viewRegistry;
        this.connect = connect;
        this.kafka = kafka;
        this.bootstrap = bootstrap;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!bootstrap.isStreams()) {
            log.info("cdcview.bootstrap.streams=false → CDC 토픽 선생성 생략");
            return;
        }
        Set<String> tables = viewRegistry.allSourceTables();
        if (tables.isEmpty()) {
            log.warn("소스 테이블이 없어 CDC 토픽을 선생성하지 않습니다");
            return;
        }
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        try (Admin admin = Admin.create(props)) {
            ensureTopics(admin, topicNames(tables));
        }
    }

    /** 뷰 정의 소스 테이블 → CDC 토픽명. TopologyBuilder가 구독하는 이름과 동일 규칙에서 파생. */
    List<String> topicNames(Collection<String> tables) {
        return tables.stream()
                .map(t -> connect.topicFor("public." + t))
                .toList();
    }

    /** 부재 토픽만 생성. 이미 존재(경합 포함)는 성공으로 취급한다 (멱등). 그 외 실패는 부팅 중단. */
    void ensureTopics(Admin admin, List<String> topics) {
        int partitions = connect.getSourcePartitions();
        try {
            Set<String> existing = admin.listTopics().names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            List<NewTopic> missing = topics.stream()
                    .filter(t -> !existing.contains(t))
                    .map(t -> new NewTopic(t, partitions, REPLICATION_FACTOR))
                    .toList();
            if (missing.isEmpty()) {
                log.info("CDC 토픽 전부 존재: {}", topics);
                return;
            }
            var result = admin.createTopics(missing,
                    new CreateTopicsOptions().timeoutMs((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
            for (NewTopic topic : missing) {
                try {
                    result.values().get(topic.name()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    log.info("CDC 토픽 선생성: {} (partitions={}, rf={})",
                            topic.name(), partitions, REPLICATION_FACTOR);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        log.info("CDC 토픽 이미 존재 (경합): {}", topic.name());
                    } else {
                        throw e;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("CDC 토픽 선생성 중단", e);
        } catch (Exception e) {
            throw new IllegalStateException("CDC 토픽 선생성 실패: " + topics, e);
        }
    }
}

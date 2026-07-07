package io.github.nini4746.cdcview.topology;

import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.insert;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.row;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.update;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.cdc.CdcMetrics;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinitionLoader;
import io.github.nini4746.cdcview.ordering.LateEventHandler;
import io.github.nini4746.cdcview.ordering.LsnGuard;
import io.github.nini4746.cdcview.ordering.OrderingMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * AC6-2 (N1): 동일 LSN(동일 커밋) 이벤트가 어떤 경로로도 집계에 2회 반영되지 않는다.
 * Debezium→Kafka 구간의 at-least-once 중복을 LSN 멱등 필터(LsnGuard)가 차단함을 고정한다.
 */
class ExactlyOnceDedupTest {

    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();
    private final OrderingMetrics orderingMetrics = new OrderingMetrics(new SimpleMeterRegistry());
    private final TopologyBuilder builder = new TopologyBuilder(
            null, new ConnectProperties(), new CdcMetrics(new SimpleMeterRegistry()),
            new LsnGuard(orderingMetrics), new LateEventHandler(orderingMetrics));
    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private TestInputTopic<String, String> setUp() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        StreamsBuilder sb = new StreamsBuilder();
        builder.buildView(sb, def, "view-seller-dashboard-v1");
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "eos-dedup");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(sb.build(), p);
        return driver.createInputTopic("cdcview.public.orders",
                new StringSerializer(), new StringSerializer());
    }

    private Map<String, Object> results() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        KeyValueStore<String, AggregateState> s = driver.getKeyValueStore("view-seller-dashboard-v1");
        return s.get("10").results(def);
    }

    @Test
    void duplicateSameLsnInsertDoesNotDoubleCount() {
        TestInputTopic<String, String> orders = setUp();
        String ev = insert("orders", 500, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0));
        orders.pipeInput("1", ev);
        orders.pipeInput("1", ev);
        orders.pipeInput("1", ev); // 3회 전달

        assertThat(results().get("order_count")).isEqualTo(1L);
        assertThat((double) results().get("total_order_amount")).isEqualTo(100.0);
    }

    @Test
    void duplicateSameLsnUpdateDoesNotDoubleApply() {
        TestInputTopic<String, String> orders = setUp();
        orders.pipeInput("1", insert("orders", 500, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));
        String upd = update("orders", 600, 2_000,
                row("id", 1L, "seller_id", 10L, "amount", 100.0),
                row("id", 1L, "seller_id", 10L, "amount", 150.0));
        orders.pipeInput("1", upd);
        orders.pipeInput("1", upd); // 동일 LSN 재전달

        assertThat((double) results().get("total_order_amount")).isEqualTo(150.0);
        assertThat(results().get("order_count")).isEqualTo(1L);
    }
}

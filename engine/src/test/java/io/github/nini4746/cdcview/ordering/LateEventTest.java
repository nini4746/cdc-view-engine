package io.github.nini4746.cdcview.ordering;

import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.insert;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.cdc.CdcMetrics;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinitionLoader;
import io.github.nini4746.cdcview.topology.AggregateState;
import io.github.nini4746.cdcview.topology.TopologyBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** AC5-3, AC5-4(N3): grace 이내 지각은 반영, grace 초과는 DLQ + 카운터. */
class LateEventTest {

    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();
    private final OrderingMetrics orderingMetrics = new OrderingMetrics(new SimpleMeterRegistry());
    private final TopologyBuilder builder = new TopologyBuilder(
            null, new ConnectProperties(), new CdcMetrics(new SimpleMeterRegistry()),
            new LsnGuard(orderingMetrics), new LateEventHandler(orderingMetrics));
    private TopologyTestDriver driver;
    private ViewDefinition def;
    private TestInputTopic<String, String> orders;
    private TestOutputTopic<String, String> dlq;

    @BeforeEach
    void setUp() {
        def = loader.loadFile(resource("view-defs/window-grace.yaml"));
        StreamsBuilder sb = new StreamsBuilder();
        builder.buildView(sb, def, "view-wingrace-v1");
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "late-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(sb.build(), p);
        orders = driver.createInputTopic("cdcview.public.orders",
                new StringSerializer(), new StringSerializer());
        dlq = driver.createOutputTopic(LateEventHandler.DLQ_TOPIC,
                new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private Map<Long, AggregateState> windowStates() {
        var store = driver.<String, AggregateState>getWindowStore("view-wingrace-v1");
        Map<Long, AggregateState> out = new HashMap<>();
        try (WindowStoreIterator<AggregateState> it =
                     store.fetch("10", Instant.ofEpochMilli(0), Instant.ofEpochMilli(300_000))) {
            while (it.hasNext()) {
                var kv = it.next();
                out.put(kv.key, kv.value);
            }
        }
        return out;
    }

    // AC5-3: grace(30s) 이내 지각 이벤트가 해당 윈도우 결과에 반영된다.
    @Test
    void withinGraceIsApplied() {
        // 윈도우 크기 60s, grace 30s. stream time을 125_000으로 올린다.
        orders.pipeInput("2", insert("orders", 1, 125_000, row("id", 2L, "seller_id", 10L, "amount", 10.0)));
        // t=100_000 이벤트: 125_000-30_000=95_000 이상이라 grace 이내 → 윈도우 [60_000,120_000)에 반영
        orders.pipeInput("1", insert("orders", 2, 100_000, row("id", 1L, "seller_id", 10L, "amount", 20.0)));

        Map<Long, AggregateState> states = windowStates();
        assertThat(states).containsKey(60_000L);
        assertThat((double) states.get(60_000L).results(def).get("total")).isEqualTo(20.0);
        assertThat(dlq.isEmpty()).isTrue();
        assertThat(orderingMetrics.lateEvents("wingrace")).isEqualTo(0.0);
    }

    // AC5-4 (N3): grace 초과 지각 이벤트는 미반영 + DLQ 적재 + 카운터 증가 (세 가지 모두).
    @Test
    void beyondGraceGoesToDlqAndCounter() {
        // stream time을 200_000으로 올린다.
        orders.pipeInput("3", insert("orders", 1, 200_000, row("id", 3L, "seller_id", 10L, "amount", 10.0)));
        // t=100_000 이벤트: 200_000-30_000=170_000 미만이라 grace 초과 → 지각
        orders.pipeInput("4", insert("orders", 2, 100_000, row("id", 4L, "seller_id", 10L, "amount", 50.0)));

        // 1) DLQ 적재
        List<String> dlqValues = dlq.readValuesToList();
        assertThat(dlqValues).hasSize(1);
        assertThat(dlqValues.get(0)).contains("\"amount\":50");
        // 2) 카운터 증가
        assertThat(orderingMetrics.lateEvents("wingrace")).isEqualTo(1.0);
        // 3) 미반영: 윈도우 [60_000,120_000)에 지각 이벤트 기여 없음
        Map<Long, AggregateState> states = windowStates();
        assertThat(states.getOrDefault(60_000L, new AggregateState()).results(def).get("cnt"))
                .isEqualTo(0L);
    }

    private static java.nio.file.Path resource(String name) {
        try {
            return java.nio.file.Path.of(
                    LateEventTest.class.getClassLoader().getResource(name).toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

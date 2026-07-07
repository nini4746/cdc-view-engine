package io.github.nini4746.cdcview.ordering;

import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.delete;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.insert;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.row;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.update;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.cdc.CdcMetrics;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinitionLoader;
import io.github.nini4746.cdcview.topology.AggregateState;
import io.github.nini4746.cdcview.topology.TopologyBuilder;
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

/** AC5-1, AC5-2: LSN 기반 stale/역순 폐기(비윈도우 경로). */
class OrderingTest {

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

    private TestInputTopic<String, String> setUpDashboard() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        StreamsBuilder sb = new StreamsBuilder();
        builder.buildView(sb, def, "view-seller-dashboard-v1");
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "ordering-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(sb.build(), p);
        return driver.createInputTopic("cdcview.public.orders",
                new StringSerializer(), new StringSerializer());
    }

    private Map<String, Object> results(String key) {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        KeyValueStore<String, AggregateState> s = driver.getKeyValueStore("view-seller-dashboard-v1");
        AggregateState st = s.get(key);
        return st == null ? null : st.results(def);
    }

    // AC5-1: 같은 키 UPDATE 2건 역순 주입 → 뷰 값은 LSN 큰 이벤트 기준.
    @Test
    void reversedUpdatesKeepHighestLsn() {
        TestInputTopic<String, String> orders = setUpDashboard();
        orders.pipeInput("1", insert("orders", 100, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));
        // LSN 큰 이벤트(300, amount 300)를 먼저, LSN 작은 이벤트(200, amount 200)를 나중에
        orders.pipeInput("1", update("orders", 300, 3_000,
                row("id", 1L, "seller_id", 10L, "amount", 100.0),
                row("id", 1L, "seller_id", 10L, "amount", 300.0)));
        orders.pipeInput("1", update("orders", 200, 2_000,
                row("id", 1L, "seller_id", 10L, "amount", 100.0),
                row("id", 1L, "seller_id", 10L, "amount", 200.0)));

        assertThat((double) results("10").get("total_order_amount")).isEqualTo(300.0);
        assertThat(orderingMetrics.staleDropped("seller-dashboard")).isEqualTo(1.0);
    }

    // AC5-2: DELETE가 UPDATE보다 먼저 도착(역순) → 행이 되살아나지 않는다.
    @Test
    void deleteBeforeLowerLsnUpdateDoesNotRevive() {
        TestInputTopic<String, String> orders = setUpDashboard();
        orders.pipeInput("1", insert("orders", 100, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));
        // DELETE(LSN 300)가 먼저 도착
        orders.pipeInput("1", delete("orders", 300, 3_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));
        // 낮은 LSN 200의 UPDATE가 뒤늦게 도착 → 폐기되어야 함
        orders.pipeInput("1", update("orders", 200, 2_000,
                row("id", 1L, "seller_id", 10L, "amount", 100.0),
                row("id", 1L, "seller_id", 10L, "amount", 200.0)));

        assertThat(results("10").get("order_count")).isEqualTo(0L);
        assertThat((double) results("10").get("total_order_amount")).isEqualTo(0.0);
        assertThat(orderingMetrics.staleDropped("seller-dashboard")).isEqualTo(1.0);
    }

    // 동일 LSN 중복 주입도 stale로 차단(Plan 07 멱등 근거).
    @Test
    void sameLsnDuplicateIsDropped() {
        TestInputTopic<String, String> orders = setUpDashboard();
        String ev = insert("orders", 100, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0));
        orders.pipeInput("1", ev);
        orders.pipeInput("1", ev); // 동일 LSN 재전달

        assertThat((double) results("10").get("total_order_amount")).isEqualTo(100.0);
        assertThat(orderingMetrics.staleDropped("seller-dashboard")).isEqualTo(1.0);
    }
}

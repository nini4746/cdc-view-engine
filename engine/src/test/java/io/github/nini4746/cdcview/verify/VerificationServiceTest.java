package io.github.nini4746.cdcview.verify;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.api.StoreAccess;
import io.github.nini4746.cdcview.cdc.ChangeEvent;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.config.ViewsProperties;
import io.github.nini4746.cdcview.topology.AggregateState;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VerificationServiceTest {

    private ViewRegistry registry;
    private ViewDefinition def;
    private AggregateState actualState;

    @BeforeEach
    void setUp() {
        ViewsProperties vp = new ViewsProperties();
        vp.setDirectory("../views");
        registry = new ViewRegistry(vp);
        registry.load();
        def = registry.find("seller-dashboard").orElseThrow();

        actualState = new AggregateState();
        actualState.apply(new ChangeEvent("orders", ChangeEvent.Op.CREATE, null,
                Map.of("seller_id", 10L, "amount", 100.0), 1, 1000), def);
        actualState.apply(new ChangeEvent("settlements", ChangeEvent.Op.CREATE, null,
                Map.of("seller_id", 10L, "settled_amount", 0.0), 2, 1000), def);
        actualState.apply(new ChangeEvent("inventory", ChangeEvent.Op.CREATE, null,
                Map.of("seller_id", 10L, "quantity", 5L), 3, 1000), def);
    }

    private VerificationService service(Map<String, Map<String, Object>> expected, long lag) {
        StoreAccess store = new FakeStore(Map.of("view-seller-dashboard-v1",
                List.of(new AbstractMap.SimpleImmutableEntry<>("10", actualState))));
        return new VerificationService(registry, d -> expected, store, () -> lag);
    }

    private Map<String, Object> matchingExpected() {
        return Map.of("order_count", 1L, "total_order_amount", 100.0,
                "total_settled_amount", 0.0, "min_inventory", 5.0);
    }

    // AC7-1/7-2: quiesced에서 일치 → verdict pass, 불일치 0.
    @Test
    void quiescedMatchIsPass() {
        VerificationReport r = service(Map.of("10", matchingExpected()), 0).verify("seller-dashboard");
        assertThat(r.quiesced()).isTrue();
        assertThat(r.verdict()).isEqualTo("pass");
        assertThat(r.mismatchCount()).isZero();
    }

    // 음성 테스트(must-have): 고의 불일치를 실제로 검출한다("항상 0" 가짜가 아님).
    @Test
    void quiescedMismatchIsDetected() {
        Map<String, Object> corrupted = new java.util.HashMap<>(matchingExpected());
        corrupted.put("order_count", 2L); // 기대 2 vs 실제 1
        VerificationReport r = service(Map.of("10", corrupted), 0).verify("seller-dashboard");
        assertThat(r.verdict()).isEqualTo("fail");
        assertThat(r.mismatchCount()).isEqualTo(1);
        assertThat(r.samples().get(0).key()).isEqualTo("10");
    }

    // 기대에는 있으나 뷰에 없는 키도 불일치로 잡는다.
    @Test
    void missingKeyInViewIsMismatch() {
        Map<String, Map<String, Object>> expected = new java.util.HashMap<>();
        expected.put("10", matchingExpected());
        expected.put("99", matchingExpected()); // 뷰에 없는 그룹
        VerificationReport r = service(expected, 0).verify("seller-dashboard");
        assertThat(r.mismatchCount()).isEqualTo(1);
        assertThat(r.samples().get(0).key()).isEqualTo("99");
    }

    // AC7-3: 비quiesced면 verdict를 내지 않는다(일치해도 null).
    @Test
    void nonQuiescedHasNoVerdict() {
        VerificationReport r = service(Map.of("10", matchingExpected()), 5).verify("seller-dashboard");
        assertThat(r.quiesced()).isFalse();
        assertThat(r.verdict()).isNull();
    }

    /** scanAll만 지원하는 store 페이크. */
    record FakeStore(Map<String, List<Map.Entry<String, AggregateState>>> data) implements StoreAccess {
        @Override
        public boolean running() {
            return true;
        }

        @Override
        public Optional<AggregateState> get(String storeName, String key) {
            return Optional.empty();
        }

        @Override
        public List<Map.Entry<Long, AggregateState>> windowFetch(
                String storeName, String key, long fromMs, long toMs) {
            return List.of();
        }

        @Override
        public List<Map.Entry<String, AggregateState>> scanAll(String storeName) {
            return data.getOrDefault(storeName, List.of());
        }
    }
}

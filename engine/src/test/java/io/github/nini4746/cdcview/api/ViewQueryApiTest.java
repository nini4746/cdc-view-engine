package io.github.nini4746.cdcview.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.nini4746.cdcview.cdc.ChangeEvent;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.config.ViewsProperties;
import io.github.nini4746.cdcview.topology.AggregateState;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class ViewQueryApiTest {

    private MockMvc mvc;
    private FakeStoreAccess store;
    private ViewRegistry registry;

    @BeforeEach
    void setUp() {
        ViewsProperties vp = new ViewsProperties();
        vp.setDirectory("../views");
        registry = new ViewRegistry(vp);
        registry.load();

        store = new FakeStoreAccess();
        // seller-dashboard, seller_id=10 에 주문 1건(amount 100) 반영
        ViewDefinition dashboard = registry.find("seller-dashboard").orElseThrow();
        AggregateState st = new AggregateState();
        st.apply(new ChangeEvent("orders", ChangeEvent.Op.CREATE, null,
                Map.of("seller_id", 10L, "amount", 100.0), 100L, 1_699_999_999_000L), dashboard);
        store.latest.put("view-seller-dashboard-v1", Map.of("10", st));

        // seller-sales-window, seller_id=10 윈도우 1개
        ViewDefinition win = registry.find("seller-sales-window").orElseThrow();
        AggregateState wst = new AggregateState();
        wst.apply(new ChangeEvent("orders", ChangeEvent.Op.CREATE, null,
                Map.of("seller_id", 10L, "amount", 50.0), 200L, 1_700_000_000_000L), win);
        store.windows.put("view-seller-sales-window-v1",
                Map.of("10", List.of(new AbstractMap.SimpleImmutableEntry<>(1_700_000_000_000L, wst))));

        ViewQueryService service = new ViewQueryService(registry, store);
        mvc = MockMvcBuilders.standaloneSetup(new ViewQueryController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    // AC4-1: 최신 집계값 + freshness 메타 JSON 반환
    @Test
    void latestReturnsValuesAndFreshness() throws Exception {
        mvc.perform(get("/views/seller-dashboard/10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.view").value("seller-dashboard"))
                .andExpect(jsonPath("$.key").value("10"))
                .andExpect(jsonPath("$.values.total_order_amount").value(100.0))
                .andExpect(jsonPath("$.values.order_count").value(1))
                .andExpect(jsonPath("$.freshness.sourceCommitTs").value(1_699_999_999_000L))
                .andExpect(jsonPath("$.freshness.appliedAt").isNumber())
                .andExpect(header().exists("ETag"));
    }

    // AC4-2: 윈도우 범위 조회 → 윈도우 목록(start/end 포함)
    @Test
    void windowRangeReturnsWindows() throws Exception {
        mvc.perform(get("/views/seller-sales-window/10?from=0&to=1800000000000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.windows[0].start").value(1_700_000_000_000L))
                .andExpect(jsonPath("$.windows[0].end").value(1_700_000_000_000L + 600_000))
                .andExpect(jsonPath("$.windows[0].values.total_order_amount").value(50.0));
    }

    // AC4-3: 없는 뷰/없는 키 → 404 + 뷰명/키 포함 바디
    @Test
    void unknownViewReturns404() throws Exception {
        mvc.perform(get("/views/nope/10"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.view").value("nope"))
                .andExpect(jsonPath("$.key").value("10"));
    }

    @Test
    void unknownKeyReturns404() throws Exception {
        mvc.perform(get("/views/seller-dashboard/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.view").value("seller-dashboard"))
                .andExpect(jsonPath("$.key").value("999"));
    }

    // AC4-4: ETag + If-None-Match 일치 시 304
    @Test
    void etagMatchReturns304() throws Exception {
        String etag = mvc.perform(get("/views/seller-dashboard/10"))
                .andReturn().getResponse().getHeader("ETag");
        mvc.perform(get("/views/seller-dashboard/10").header("If-None-Match", etag))
                .andExpect(status().isNotModified());
    }

    // 윈도우 뷰에 range 없으면 400
    @Test
    void windowViewWithoutRangeReturns400() throws Exception {
        mvc.perform(get("/views/seller-sales-window/10"))
                .andExpect(status().isBadRequest());
    }

    // 비윈도우 뷰에 range 주면 400
    @Test
    void nonWindowViewWithRangeReturns400() throws Exception {
        mvc.perform(get("/views/seller-dashboard/10?from=0&to=100"))
                .andExpect(status().isBadRequest());
    }

    /** running=true, 맵 기반 store 페이크. */
    static class FakeStoreAccess implements StoreAccess {
        final Map<String, Map<String, AggregateState>> latest = new HashMap<>();
        final Map<String, Map<String, List<Map.Entry<Long, AggregateState>>>> windows = new HashMap<>();

        @Override
        public boolean running() {
            return true;
        }

        @Override
        public Optional<AggregateState> get(String storeName, String key) {
            return Optional.ofNullable(latest.getOrDefault(storeName, Map.of()).get(key));
        }

        @Override
        public List<Map.Entry<Long, AggregateState>> windowFetch(
                String storeName, String key, long fromMs, long toMs) {
            return windows.getOrDefault(storeName, Map.of()).getOrDefault(key, List.of());
        }
    }
}

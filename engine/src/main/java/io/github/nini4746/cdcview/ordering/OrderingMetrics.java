package io.github.nini4746.cdcview.ordering;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/** 순서 처리 관측 지표: stale 폐기(view.stale.dropped), grace 초과 지각(view.late.events). */
@Component
public class OrderingMetrics {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, Counter> stale = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> late = new ConcurrentHashMap<>();

    public OrderingMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void incStaleDropped(String view) {
        stale.computeIfAbsent(view, v -> Counter.builder("view.stale.dropped")
                .description("LSN이 낮아 폐기된 stale/중복 이벤트 수")
                .tag("view", v).register(registry)).increment();
    }

    public void incLateEvents(String view) {
        late.computeIfAbsent(view, v -> Counter.builder("view.late.events")
                .description("grace 초과로 DLQ에 적재된 지각 이벤트 수")
                .tag("view", v).register(registry)).increment();
    }

    public double staleDropped(String view) {
        Counter c = stale.get(view);
        return c == null ? 0 : c.count();
    }

    public double lateEvents(String view) {
        Counter c = late.get(view);
        return c == null ? 0 : c.count();
    }
}

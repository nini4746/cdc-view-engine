package io.github.nini4746.cdcview.cdc;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * CDC/뷰 반영 관측 지표.
 * - cdc.events.received: 테이블별 수신 카운터
 * - view.apply.latency: PG 커밋(ts_ms) 대비 뷰 반영 시각의 지연 분포(p50/p95/p99), 뷰별 태그 (Plan 11)
 */
@Component
public class CdcMetrics {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, Counter> received = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> applyLatency = new ConcurrentHashMap<>();

    public CdcMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordReceived(String table) {
        received.computeIfAbsent(table, t ->
                Counter.builder("cdc.events.received")
                        .description("Debezium에서 수신해 파싱한 CDC 이벤트 수")
                        .tag("table", t)
                        .register(registry)).increment();
    }

    /** 커밋 시각 대비 뷰 반영 지연(ms) 기록. */
    public void recordApplyLatency(String view, long latencyMs) {
        applyLatency.computeIfAbsent(view, v ->
                Timer.builder("view.apply.latency")
                        .description("PG 커밋 대비 뷰 반영 지연")
                        .tag("view", v)
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .register(registry)).record(Duration.ofMillis(Math.max(0, latencyMs)));
    }
}

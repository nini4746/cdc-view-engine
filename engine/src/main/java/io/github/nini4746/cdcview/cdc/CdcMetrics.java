package io.github.nini4746.cdcview.cdc;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/** CDC 수신 관측 지표. 테이블별 수신 카운터(cdc.events.received). */
@Component
public class CdcMetrics {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, Counter> received = new ConcurrentHashMap<>();

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
}

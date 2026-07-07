package io.github.nini4746.cdcview.topology;

import io.github.nini4746.cdcview.cdc.CdcMetrics;
import io.github.nini4746.cdcview.cdc.ChangeEvent;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.ordering.LateEventHandler;
import io.github.nini4746.cdcview.ordering.LsnGuard;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.stereotype.Component;

/**
 * ViewDefinition으로부터 Kafka Streams 서브토폴로지를 동적으로 구성한다. 뷰별 하드코딩 없음.
 *
 * <p>설계: 소스 테이블별 CDC 스트림을 그룹 키로 리키잉해 병합한 뒤, 하나의 aggregate로 뷰를 구체화한다.
 * 증분 가산/감산은 {@link AggregateState}가 before/after 이미지로 수행한다(UPDATE=감산+가산, DELETE=감산).
 */
@Component
public class TopologyBuilder {

    private final ViewRegistry registry;
    private final ConnectProperties connect;
    private final CdcMetrics metrics;
    private final LsnGuard lsnGuard;
    private final LateEventHandler lateEventHandler;

    public TopologyBuilder(ViewRegistry registry, ConnectProperties connect, CdcMetrics metrics,
                           LsnGuard lsnGuard, LateEventHandler lateEventHandler) {
        this.registry = registry;
        this.connect = connect;
        this.metrics = metrics;
        this.lsnGuard = lsnGuard;
        this.lateEventHandler = lateEventHandler;
    }

    public void build(StreamsBuilder builder) {
        for (ViewDefinition def : registry.all()) {
            buildView(builder, def, registry.currentStoreName(def.getName()));
        }
    }

    /** 단일 뷰를 지정 store 이름으로 구체화한다(Plan 09 섀도 빌드에서도 재사용). */
    public void buildView(StreamsBuilder builder, ViewDefinition def, String storeName) {
        Serde<ChangeEvent> sourceSerde = ChangeEventSerdes.forSourceTopic();
        Serde<ChangeEvent> jsonSerde = ChangeEventSerdes.json();
        Serde<AggregateState> stateSerde = new JsonSerde<>(AggregateState.class);

        List<KStream<String, ChangeEvent>> perSource = new ArrayList<>();
        for (ViewDefinition.Source src : def.getSources()) {
            String keyCol = Optional.ofNullable(src.getJoinKey()).orElse(def.getGroupBy());
            String topic = connect.topicFor("public." + src.getTable());

            KStream<String, ChangeEvent> s = builder
                    .stream(topic, Consumed.with(Serdes.String(), sourceSerde)
                            .withTimestampExtractor(new EventTimeExtractor()))
                    .filter((k, ev) -> ev != null)
                    .peek((k, ev) -> metrics.recordReceived(ev.table()));
            // 소스 PK 키 유지 상태로 LSN 가드 적용(stale/중복 폐기) 후 그룹 키로 리키잉
            s = lsnGuard.apply(s, def.getName(), src.getTable())
                    .selectKey((k, ev) -> groupKey(ev, keyCol));
            perSource.add(s);
        }

        KStream<String, ChangeEvent> merged = perSource.get(0);
        for (int i = 1; i < perSource.size(); i++) {
            merged = merged.merge(perSource.get(i));
        }

        // 윈도우 뷰: grace 초과 지각 이벤트를 DLQ로 보내고 정시 이벤트만 집계
        if (def.isWindowed()) {
            merged = lateEventHandler.apply(merged, def);
        }

        KGroupedStream<String, ChangeEvent> grouped =
                merged.groupByKey(Grouped.with(Serdes.String(), jsonSerde));

        // 집계 반영 시 커밋 대비 지연 히스토그램 기록(Plan 11 벤치 기준점: PG 커밋 시각)
        org.apache.kafka.streams.kstream.Aggregator<String, ChangeEvent, AggregateState> aggregator =
                (key, ev, agg) -> {
                    AggregateState s = agg.apply(ev, def);
                    metrics.recordApplyLatency(def.getName(), System.currentTimeMillis() - ev.tsMs());
                    return s;
                };

        if (def.isWindowed()) {
            TimeWindows windows = timeWindows(def.getWindow());
            grouped.windowedBy(windows)
                    .aggregate(AggregateState::new, aggregator,
                            Materialized.<String, AggregateState, WindowStore<Bytes, byte[]>>as(storeName)
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(stateSerde));
        } else {
            grouped.aggregate(AggregateState::new, aggregator,
                    Materialized.<String, AggregateState, KeyValueStore<Bytes, byte[]>>as(storeName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(stateSerde));
        }
    }

    private static String groupKey(ChangeEvent ev, String keyCol) {
        Map<String, Object> image = ev.effectiveImage();
        Object v = image == null ? null : image.get(keyCol);
        return String.valueOf(v);
    }

    private static TimeWindows timeWindows(ViewDefinition.Window w) {
        Duration size = w.getSize();
        Duration grace = w.getGrace();
        if (w.getType() == ViewDefinition.WindowType.SLIDING) {
            // sliding(hopping): advanceBy로 겹치는 윈도우 생성
            return TimeWindows.ofSizeAndGrace(size, grace).advanceBy(w.getAdvance());
        }
        // tumbling: advance == size (기본)
        return TimeWindows.ofSizeAndGrace(size, grace);
    }
}

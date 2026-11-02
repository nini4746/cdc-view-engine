package io.github.nini4746.cdcview.api;

import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinition.AggregateFunction;
import io.github.nini4746.cdcview.config.ViewDefinition.Feature;
import io.github.nini4746.cdcview.config.ViewDefinition.SubjectType;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.topology.AggregateState;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 뷰 YAML의 features 선언을 따라 피처 값을 조회한다.
 *
 * <p>값은 지금을 포함하는 윈도우 중 가장 오래된 것, 즉 최근 구간을 가장 많이 덮는 윈도우에서 읽는다.
 * sliding 뷰의 정밀도는 advance 단위이고, tumbling 뷰는 현재 고정 구간(예: UTC 당일)이다.
 */
@Service
public class FeatureQueryService {

    private final ViewRegistry registry;
    private final StoreAccess storeAccess;
    private final Clock clock;

    @Autowired
    public FeatureQueryService(ViewRegistry registry, StoreAccess storeAccess) {
        this(registry, storeAccess, Clock.systemUTC());
    }

    FeatureQueryService(ViewRegistry registry, StoreAccess storeAccess, Clock clock) {
        this.registry = registry;
        this.storeAccess = storeAccess;
        this.clock = clock;
    }

    public FeatureQueryResponse query(FeatureQueryRequest request) {
        SubjectType subject = parseSubject(request);
        if (request.subjectKey() == null || request.subjectKey().isBlank()) {
            throw new BadQueryException("subjectKey가 필요합니다", "features", null);
        }
        if (request.features() == null) {
            throw new BadQueryException("features가 필요합니다", "features", request.subjectKey());
        }
        if (!storeAccess.running()) {
            throw new StreamsNotReadyException("Streams가 RUNNING 상태가 아닙니다");
        }

        Map<String, Double> values = new LinkedHashMap<>();
        List<String> missing = new ArrayList<>();
        for (FeatureRef ref : request.features()) {
            String reference = Feature.referenceKey(subject, ref.name(), ref.window());
            Optional<Double> value = read(reference, request.subjectKey());
            if (value.isPresent()) {
                values.put(reference, value.get());
            } else {
                missing.add(reference);
            }
        }
        return new FeatureQueryResponse(values, missing);
    }

    private static SubjectType parseSubject(FeatureQueryRequest request) {
        try {
            return SubjectType.valueOf(String.valueOf(request.subjectType()));
        } catch (IllegalArgumentException e) {
            throw new BadQueryException("subjectType은 ACCOUNT, IP, CARD 중 하나여야 합니다",
                    "features", request.subjectKey());
        }
    }

    private Optional<Double> read(String reference, String subjectKey) {
        for (ViewDefinition def : registry.all()) {
            for (Feature feature : def.getFeatures()) {
                if (feature.referenceKey().equals(reference)) {
                    return readFeature(def, feature, subjectKey);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<Double> readFeature(ViewDefinition def, Feature feature, String subjectKey) {
        long now = clock.millis();
        long sizeMs = def.getWindow().getSize().toMillis();
        String store = registry.currentStoreName(def.getName());
        Optional<AggregateState> state = storeAccess.windowFetch(store, subjectKey, now - sizeMs + 1, now).stream()
                .filter(row -> row.getKey() > now - sizeMs && row.getKey() <= now)
                .min(Comparator.comparingLong(Map.Entry::getKey))
                .map(Map.Entry::getValue);
        if (state.isEmpty()) {
            return emptyWindowValue(def, feature);
        }
        Object value = state.get().results(def).get(feature.getField());
        return value instanceof Number number ? Optional.of(number.doubleValue()) : Optional.empty();
    }

    /** 구간에 행이 없을 때 합계와 개수는 0이 맞지만, min/max/avg는 값이 없다. */
    private static Optional<Double> emptyWindowValue(ViewDefinition def, Feature feature) {
        AggregateFunction function = def.getAggregations().stream()
                .filter(agg -> agg.getAs().equals(feature.getField()))
                .findFirst().orElseThrow().getFunction();
        return switch (function) {
            case COUNT, COUNT_DISTINCT, SUM -> Optional.of(0.0);
            case MIN, MAX, AVG -> Optional.empty();
        };
    }
}

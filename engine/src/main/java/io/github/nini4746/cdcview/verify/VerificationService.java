package io.github.nini4746.cdcview.verify;

import io.github.nini4746.cdcview.api.StoreAccess;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.topology.AggregateState;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.springframework.stereotype.Service;

/**
 * 소스 스냅샷 기대값과 서빙 뷰를 비교해 불일치를 리포트한다.
 *
 * <p>비블로킹: store 스캔은 read-only라 서빙 조회와 경합하지 않는다(별도 락 없음).
 * quiesced가 아니면 verdict를 내지 않는다(거짓 불일치 방지, AC7-3).
 */
@Service
public class VerificationService {

    private static final double EPS = 1e-6;

    private final ViewRegistry registry;
    private final ExpectedViewProvider expectedProvider;
    private final StoreAccess storeAccess;
    private final QuiesceChecker quiesceChecker;

    public VerificationService(ViewRegistry registry, ExpectedViewProvider expectedProvider,
                               StoreAccess storeAccess, QuiesceChecker quiesceChecker) {
        this.registry = registry;
        this.expectedProvider = expectedProvider;
        this.storeAccess = storeAccess;
        this.quiesceChecker = quiesceChecker;
    }

    public VerificationReport verify(String viewName) {
        long start = System.currentTimeMillis();
        ViewDefinition def = registry.find(viewName)
                .orElseThrow(() -> new IllegalArgumentException("없는 뷰: " + viewName));

        boolean quiesced = quiesceChecker.isQuiesced();

        Map<String, Map<String, Object>> expected = expectedProvider.compute(def);
        Map<String, Map<String, Object>> actual = scanActual(def, viewName);

        List<VerificationReport.Mismatch> mismatches = compare(def, expected, actual);

        String verdict = quiesced ? (mismatches.isEmpty() ? "pass" : "fail") : null;
        List<VerificationReport.Mismatch> samples =
                mismatches.subList(0, Math.min(10, mismatches.size()));

        return new VerificationReport(viewName, quiesced, verdict, mismatches.size(),
                new ArrayList<>(samples), expected.size(), actual.size(),
                System.currentTimeMillis() - start);
    }

    private Map<String, Map<String, Object>> scanActual(ViewDefinition def, String viewName) {
        String store = registry.currentStoreName(viewName);
        Map<String, Map<String, Object>> actual = new LinkedHashMap<>();
        for (Map.Entry<String, AggregateState> e : storeAccess.scanAll(store)) {
            actual.put(e.getKey(), e.getValue().results(def));
        }
        return actual;
    }

    private List<VerificationReport.Mismatch> compare(
            ViewDefinition def,
            Map<String, Map<String, Object>> expected,
            Map<String, Map<String, Object>> actual) {

        Set<String> keys = new TreeSet<>();
        keys.addAll(expected.keySet());
        keys.addAll(actual.keySet());

        List<String> fields = def.getAggregations().stream().map(ViewDefinition.Aggregation::getAs).toList();
        List<VerificationReport.Mismatch> mismatches = new ArrayList<>();
        for (String key : keys) {
            Map<String, Object> exp = expected.get(key);
            Map<String, Object> act = actual.get(key);
            if (exp == null || act == null || !fieldsEqual(fields, exp, act)) {
                mismatches.add(new VerificationReport.Mismatch(key, exp, act));
            }
        }
        return mismatches;
    }

    private boolean fieldsEqual(List<String> fields, Map<String, Object> exp, Map<String, Object> act) {
        for (String f : fields) {
            if (!numericEqual(exp.get(f), act.get(f))) {
                return false;
            }
        }
        return true;
    }

    private boolean numericEqual(Object a, Object b) {
        Double da = toDouble(a);
        Double db = toDouble(b);
        if (da == null || db == null) {
            return da == null && db == null;
        }
        return Math.abs(da - db) < EPS;
    }

    private Double toDouble(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(o.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

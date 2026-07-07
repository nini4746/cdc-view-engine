package io.github.nini4746.cdcview.api;

import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.topology.AggregateState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * 뷰 조회 서비스. 뷰명 → store 해석은 ViewRegistry에 집중(Plan 09 스왑 대비).
 * 응답과 ETag를 함께 만든다. ETag는 뷰 버전 + 마지막 반영 LSN 기반.
 */
@Service
public class ViewQueryService {

    private final ViewRegistry registry;
    private final StoreAccess storeAccess;

    public ViewQueryService(ViewRegistry registry, StoreAccess storeAccess) {
        this.registry = registry;
        this.storeAccess = storeAccess;
    }

    /** 응답 + ETag 래퍼. */
    public record Served<T>(T body, String etag) {
    }

    public Served<ViewResponse> getLatest(String viewName, String key) {
        ViewDefinition def = requireView(viewName, key);
        if (def.isWindowed()) {
            throw new BadQueryException("윈도우 뷰는 from/to 범위 파라미터가 필요합니다", viewName, key);
        }
        requireRunning();

        int version = registry.currentVersion(viewName);
        String store = registry.currentStoreName(viewName);
        Optional<AggregateState> state = storeAccess.get(store, key);
        if (state.isEmpty()) {
            throw new ViewNotFoundException("키를 찾을 수 없습니다", viewName, key);
        }
        AggregateState st = state.get();
        ViewResponse body = new ViewResponse(viewName, key, st.results(def),
                new Freshness(st.getLastCommitTs(), st.getLastAppliedAt()), version);
        String etag = "\"v" + version + "-" + st.getLastLsn() + "\"";
        return new Served<>(body, etag);
    }

    public Served<WindowResponse> getWindowRange(String viewName, String key, long fromMs, long toMs) {
        ViewDefinition def = requireView(viewName, key);
        if (!def.isWindowed()) {
            throw new BadQueryException("비윈도우 뷰에는 from/to 범위를 쓸 수 없습니다", viewName, key);
        }
        if (fromMs > toMs) {
            throw new BadQueryException("from이 to보다 큽니다", viewName, key);
        }
        requireRunning();

        int version = registry.currentVersion(viewName);
        String store = registry.currentStoreName(viewName);
        long windowSizeMs = def.getWindow().getSize().toMillis();

        List<Map.Entry<Long, AggregateState>> rows = storeAccess.windowFetch(store, key, fromMs, toMs);
        if (rows.isEmpty()) {
            throw new ViewNotFoundException("범위 내 윈도우 결과가 없습니다", viewName, key);
        }
        List<WindowEntry> windows = new ArrayList<>();
        long maxLsn = -1;
        for (Map.Entry<Long, AggregateState> row : rows) {
            long start = row.getKey();
            AggregateState st = row.getValue();
            windows.add(new WindowEntry(start, start + windowSizeMs, st.results(def),
                    new Freshness(st.getLastCommitTs(), st.getLastAppliedAt())));
            maxLsn = Math.max(maxLsn, st.getLastLsn());
        }
        WindowResponse body = new WindowResponse(viewName, key, windows);
        String etag = "\"v" + version + "-w" + maxLsn + "\"";
        return new Served<>(body, etag);
    }

    private ViewDefinition requireView(String viewName, String key) {
        return registry.find(viewName)
                .orElseThrow(() -> new ViewNotFoundException("뷰를 찾을 수 없습니다", viewName, key));
    }

    private void requireRunning() {
        if (!storeAccess.running()) {
            throw new StreamsNotReadyException("Streams가 RUNNING 상태가 아닙니다");
        }
    }
}

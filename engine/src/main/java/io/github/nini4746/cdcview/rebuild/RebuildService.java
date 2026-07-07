package io.github.nini4746.cdcview.rebuild;

import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 스키마 변경 중 무중단 뷰 재구축. 섀도 빌드 + 원자적 스왑.
 *
 * <p>파이프라인: (1) 뷰 정의 재로딩 + 참조 컬럼 존재 검증 → (2) 새 버전 store로 섀도 빌드 →
 * (3) 라이브 따라잡기 감시 → (4) 레지스트리 서빙 포인터를 원자적으로 교체(단일 volatile 참조) →
 * (5) 구버전 정리. 스왑은 다단계 부분 스왑이 아닌 단일 참조 교체다(AC8-3의 무혼합 보장).
 * 실패/중단 시 기존 뷰 서빙에 영향이 없다.
 */
@Service
public class RebuildService {

    private static final Logger log = LoggerFactory.getLogger(RebuildService.class);

    private final ViewRegistry registry;
    private final ColumnChecker columnChecker;
    private final ShadowBuilder shadowBuilder;

    private final ConcurrentMap<String, RebuildState> states = new ConcurrentHashMap<>();
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "rebuild-worker");
                t.setDaemon(true);
                return t;
            });

    // 따라잡기 폴링 파라미터
    private long pollIntervalMs = 500;
    private int maxPollAttempts = 600; // 최대 5분

    public RebuildService(ViewRegistry registry, ColumnChecker columnChecker, ShadowBuilder shadowBuilder) {
        this.registry = registry;
        this.columnChecker = columnChecker;
        this.shadowBuilder = shadowBuilder;
    }

    /** 재구축 요청. 검증 후 비동기로 파이프라인을 실행하고 현재 상태를 반환한다. */
    public RebuildState rebuild(String view) {
        RebuildState existing = states.get(view);
        if (existing != null && !existing.isTerminal()) {
            throw new IllegalStateException("이미 진행 중인 재구축이 있습니다: " + view);
        }
        // 뷰 정의 재로딩(편집된 YAML 반영) 후 대상 뷰 확인
        registry.reload();
        ViewDefinition def = registry.find(view)
                .orElseThrow(() -> new IllegalArgumentException("없는 뷰: " + view));

        int fromVersion = registry.currentVersion(view);
        int toVersion = fromVersion + 1;
        long now = System.currentTimeMillis();
        RebuildState state = new RebuildState(view, RebuildState.Status.VALIDATING,
                fromVersion, toVersion, "참조 컬럼 검증 중", now, now);
        states.put(view, state);

        // 참조 컬럼 존재 검증은 동기로 수행(즉시 400 반환 위해)
        columnChecker.verify(def);

        executor.submit(() -> runPipeline(def, view, toVersion));
        return states.get(view);
    }

    private void runPipeline(ViewDefinition def, String view, int toVersion) {
        try {
            update(view, RebuildState.Status.SNAPSHOTTING, "섀도 빌드 기동");
            shadowBuilder.startShadow(def, toVersion);

            update(view, RebuildState.Status.CATCHING_UP, "라이브 따라잡기 대기");
            int attempts = 0;
            while (!shadowBuilder.caughtUp(view, toVersion)) {
                if (++attempts > maxPollAttempts) {
                    throw new IllegalStateException("따라잡기 타임아웃");
                }
                Thread.sleep(pollIntervalMs);
            }

            // 원자적 스왑: 승격 후 서빙 포인터 교체(단일 참조)
            shadowBuilder.promote(view, toVersion);
            registry.swapVersion(view, toVersion);
            update(view, RebuildState.Status.SWAPPED, "v" + toVersion + " 스왑 완료");

            // 구버전 정리
            shadowBuilder.discard(view, toVersion - 1);
            log.info("뷰 '{}' 재구축 완료 → v{}", view, toVersion);
        } catch (Exception e) {
            log.error("뷰 '{}' 재구축 실패: {}", view, e.getMessage());
            shadowBuilder.discard(view, toVersion); // 미완 섀도 정리, 구버전 서빙 유지
            update(view, RebuildState.Status.FAILED, "실패: " + e.getMessage());
        }
    }

    private void update(String view, RebuildState.Status status, String msg) {
        states.computeIfPresent(view, (k, s) -> s.withStatus(status, msg));
    }

    public RebuildState status(String view) {
        RebuildState s = states.get(view);
        if (s != null) {
            return s;
        }
        return new RebuildState(view, RebuildState.Status.IDLE,
                registry.currentVersion(view), registry.currentVersion(view),
                "진행 중인 재구축 없음", 0, 0);
    }

    // 테스트용 폴링 파라미터 조정
    void setPollParams(long intervalMs, int maxAttempts) {
        this.pollIntervalMs = intervalMs;
        this.maxPollAttempts = maxAttempts;
    }
}

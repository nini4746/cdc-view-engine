package io.github.nini4746.cdcview.api;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 피처 조회 REST 엔드포인트. ticket-fraud-engine의 RestFeatureStoreClient가 소비하는 계약이다.
 *
 * <p>POST /api/v1/features/query
 *
 * <p>제공할 수 없는 피처는 에러가 아니라 missing으로 응답한다. 잘못된 요청은 400, Streams 미준비 시 503.
 */
@RestController
public class FeatureQueryController {

    private final FeatureQueryService service;

    public FeatureQueryController(FeatureQueryService service) {
        this.service = service;
    }

    @PostMapping("/api/v1/features/query")
    public FeatureQueryResponse query(@RequestBody FeatureQueryRequest request) {
        return service.query(request);
    }
}

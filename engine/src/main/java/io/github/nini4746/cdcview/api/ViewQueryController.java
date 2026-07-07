package io.github.nini4746.cdcview.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 뷰 조회 REST 엔드포인트.
 *
 * <p>GET /views/{view}/{key}                     - 비윈도우 뷰 최신 값 + freshness
 * <p>GET /views/{view}/{key}?from={ms}&to={ms}   - 윈도우 뷰 시간 범위 결과 목록
 *
 * <p>응답에 ETag 포함, If-None-Match 일치 시 304. 없는 뷰/키는 404, 잘못된 파라미터는 400,
 * Streams 미준비 시 503.
 */
@RestController
public class ViewQueryController {

    private final ViewQueryService service;

    public ViewQueryController(ViewQueryService service) {
        this.service = service;
    }

    @GetMapping("/views/{view}/{key}")
    public ResponseEntity<?> query(
            @PathVariable String view,
            @PathVariable String key,
            @RequestParam(required = false) Long from,
            @RequestParam(required = false) Long to,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {

        if (from != null || to != null) {
            if (from == null || to == null) {
                throw new BadQueryException("윈도우 범위 조회는 from과 to가 모두 필요합니다", view, key);
            }
            ViewQueryService.Served<WindowResponse> served = service.getWindowRange(view, key, from, to);
            return respond(served.etag(), ifNoneMatch, served.body());
        }
        ViewQueryService.Served<ViewResponse> served = service.getLatest(view, key);
        return respond(served.etag(), ifNoneMatch, served.body());
    }

    private ResponseEntity<?> respond(String etag, String ifNoneMatch, Object body) {
        if (etag.equals(ifNoneMatch)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(etag).build();
        }
        return ResponseEntity.ok().eTag(etag).body(body);
    }
}

package io.github.nini4746.cdcview.rebuild;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 뷰 재구축 트리거/상태 조회.
 * POST /admin/views/{view}/rebuild
 * GET  /admin/views/{view}/rebuild/status
 */
@RestController
public class RebuildController {

    private final RebuildService service;

    public RebuildController(RebuildService service) {
        this.service = service;
    }

    @PostMapping("/admin/views/{view}/rebuild")
    public ResponseEntity<RebuildState> rebuild(@PathVariable String view) {
        return ResponseEntity.accepted().body(service.rebuild(view));
    }

    @GetMapping("/admin/views/{view}/rebuild/status")
    public RebuildState status(@PathVariable String view) {
        return service.status(view);
    }

    @ExceptionHandler(RebuildValidationException.class)
    public ResponseEntity<String> validation(RebuildValidationException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> notFound(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
    }
}

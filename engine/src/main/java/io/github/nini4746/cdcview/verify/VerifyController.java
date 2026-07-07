package io.github.nini4746.cdcview.verify;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 정합성 검증 온디맨드 트리거. MVP는 동기 응답.
 * POST /admin/verify/{view}
 */
@RestController
public class VerifyController {

    private final VerificationService service;

    public VerifyController(VerificationService service) {
        this.service = service;
    }

    @PostMapping("/admin/verify/{view}")
    public VerificationReport verify(@PathVariable String view) {
        return service.verify(view);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> badView(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }

    @ExceptionHandler(UnsupportedOperationException.class)
    public ResponseEntity<String> unsupported(UnsupportedOperationException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }
}

package io.github.nini4746.cdcview.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** REST 예외 → HTTP 상태 매핑. 없는 리소스에 200+null을 반환하지 않는다. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(ViewNotFoundException.class)
    public ResponseEntity<ErrorResponse> notFound(ViewNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse(e.getMessage(), e.view(), e.key()));
    }

    @ExceptionHandler(BadQueryException.class)
    public ResponseEntity<ErrorResponse> badRequest(BadQueryException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse(e.getMessage(), e.view(), e.key()));
    }

    @ExceptionHandler(StreamsNotReadyException.class)
    public ResponseEntity<ErrorResponse> notReady(StreamsNotReadyException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse(e.getMessage(), null, null));
    }
}

package io.github.nini4746.cdcview.api;

/** 미등록 뷰 또는 없는 키 → 404. */
class ViewNotFoundException extends RuntimeException {
    private final String view;
    private final String key;

    ViewNotFoundException(String message, String view, String key) {
        super(message);
        this.view = view;
        this.key = key;
    }

    String view() {
        return view;
    }

    String key() {
        return key;
    }
}

/** 잘못된 조회 파라미터(윈도우 뷰 range 누락 등) → 400. */
class BadQueryException extends RuntimeException {
    private final String view;
    private final String key;

    BadQueryException(String message, String view, String key) {
        super(message);
        this.view = view;
        this.key = key;
    }

    String view() {
        return view;
    }

    String key() {
        return key;
    }
}

/** Streams가 RUNNING이 아님 → 503(500 아님). */
class StreamsNotReadyException extends RuntimeException {
    StreamsNotReadyException(String message) {
        super(message);
    }

    StreamsNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}

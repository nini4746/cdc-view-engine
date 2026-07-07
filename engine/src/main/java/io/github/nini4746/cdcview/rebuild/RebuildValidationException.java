package io.github.nini4746.cdcview.rebuild;

/** 재구축 사전 검증 실패(참조 컬럼 없음 등) → 400. */
public class RebuildValidationException extends RuntimeException {
    public RebuildValidationException(String message) {
        super(message);
    }
}

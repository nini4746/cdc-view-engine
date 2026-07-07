package io.github.nini4746.cdcview.verify;

import java.util.List;
import java.util.Map;

/**
 * 정합성 검증 리포트.
 *
 * @param view          뷰명
 * @param quiesced      입력 정지 + consumer lag 0 여부
 * @param verdict       "pass"|"fail". quiesced가 아니면 null(판정하지 않음, AC7-3)
 * @param mismatchCount 불일치 그룹 키 수
 * @param samples       불일치 샘플 최대 10건
 * @param expectedGroups 기대 그룹 수
 * @param actualGroups   실제 뷰 그룹 수
 * @param durationMs    소요 시간
 */
public record VerificationReport(
        String view,
        boolean quiesced,
        String verdict,
        int mismatchCount,
        List<Mismatch> samples,
        int expectedGroups,
        int actualGroups,
        long durationMs) {

    /** 불일치 1건. */
    public record Mismatch(String key, Map<String, Object> expected, Map<String, Object> actual) {
    }
}

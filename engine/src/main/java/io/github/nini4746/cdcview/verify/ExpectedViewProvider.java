package io.github.nini4746.cdcview.verify;

import io.github.nini4746.cdcview.config.ViewDefinition;
import java.util.Map;

/** 소스 PG 스냅샷에서 뷰 기대값을 계산한다(그룹 키 → 필드값 맵). */
public interface ExpectedViewProvider {
    Map<String, Map<String, Object>> compute(ViewDefinition def);
}

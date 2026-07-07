package io.github.nini4746.cdcview.config;

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/**
 * 뷰 정의 오류를 부팅 실패 화면에 파일명/필드 경로와 함께 명확히 출력한다(AC1-2).
 */
public class ViewDefinitionFailureAnalyzer extends AbstractFailureAnalyzer<ViewDefinitionException> {

    @Override
    protected FailureAnalysis analyze(Throwable rootFailure, ViewDefinitionException cause) {
        String description = cause.getMessage();
        String action = "해당 YAML 파일의 필드를 수정한 뒤 다시 기동하세요.";
        return new FailureAnalysis(description, action, cause);
    }
}

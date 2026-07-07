package io.github.nini4746.cdcview.config;

/**
 * 뷰 정의 로딩/검증 실패. 메시지에 파일명과 필드 경로를 포함한다(AC1-2).
 * 부팅 단계에서 던져지면 컨텍스트 초기화가 실패해 프로세스가 비정상 종료(exit != 0)된다.
 */
public class ViewDefinitionException extends RuntimeException {

    private final String file;
    private final String fieldPath;

    public ViewDefinitionException(String file, String fieldPath, String reason) {
        super(format(file, fieldPath, reason));
        this.file = file;
        this.fieldPath = fieldPath;
    }

    public ViewDefinitionException(String file, String fieldPath, String reason, Throwable cause) {
        super(format(file, fieldPath, reason), cause);
        this.file = file;
        this.fieldPath = fieldPath;
    }

    private static String format(String file, String fieldPath, String reason) {
        StringBuilder sb = new StringBuilder("뷰 정의 오류");
        if (file != null) {
            sb.append(" [file=").append(file).append("]");
        }
        if (fieldPath != null) {
            sb.append(" [field=").append(fieldPath).append("]");
        }
        sb.append(": ").append(reason);
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public String getFieldPath() {
        return fieldPath;
    }
}

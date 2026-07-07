package io.github.nini4746.cdcview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** cdcview.views.* 설정. */
@ConfigurationProperties(prefix = "cdcview.views")
public class ViewsProperties {

    /** 뷰 정의 YAML 디렉토리. */
    private String directory = "views";

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }
}

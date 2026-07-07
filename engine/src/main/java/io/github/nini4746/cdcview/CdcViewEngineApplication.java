package io.github.nini4746.cdcview;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * CDC 실시간 구체화 뷰 엔진 진입점.
 *
 * <p>단일 인스턴스 전제. Kafka Streams state store(RocksDB)에 뷰를 구체화하고 동일 프로세스에서
 * REST로 서빙한다. Kafka Streams 라이프사이클은 자동설정에 맡기지 않고 직접 관리한다(Plan 04).
 */
// DataSource는 cdcview.postgres 설정에서 직접 구성하므로 자동설정을 제외한다.
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class CdcViewEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(CdcViewEngineApplication.class, args);
    }
}

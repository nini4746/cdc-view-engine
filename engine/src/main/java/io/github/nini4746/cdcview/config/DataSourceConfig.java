package io.github.nini4746.cdcview.config;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * cdcview.postgres 설정으로 소스 PG DataSource를 직접 구성한다(자동설정 제외).
 * PG가 없어도 부팅이 실패하지 않도록 초기 연결 검증을 생략한다(initializationFailTimeout=-1).
 */
@Configuration
public class DataSourceConfig {

    @Bean
    public DataSource sourceDataSource(PostgresProperties props) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(props.jdbcUrl());
        ds.setUsername(props.getUsername());
        ds.setPassword(props.getPassword());
        ds.setMaximumPoolSize(4);
        ds.setPoolName("cdcview-source");
        // 부팅 시 PG 미가용이어도 컨텍스트가 뜨도록 초기 연결 검증 생략
        ds.setInitializationFailTimeout(-1);
        return ds;
    }

    @Bean
    public JdbcTemplate sourceJdbcTemplate(DataSource sourceDataSource) {
        return new JdbcTemplate(sourceDataSource);
    }

    @Bean
    public PlatformTransactionManager sourceTransactionManager(DataSource sourceDataSource) {
        return new DataSourceTransactionManager(sourceDataSource);
    }
}

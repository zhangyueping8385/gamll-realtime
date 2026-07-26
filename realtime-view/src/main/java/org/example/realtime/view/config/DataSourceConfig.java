package org.example.realtime.view.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Value("${doris.fenodes}")
    private String feNodes;

    @Value("${doris.database}")
    private String database;

    @Value("${doris.username}")
    private String username;

    @Value("${doris.password}")
    private String password;

    @Bean
    public DataSource dataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        String jdbcUrl = String.format("jdbc:mysql://%s/%s?connectTimeout=120000&socketTimeout=120000", feNodes, database);
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setConnectionTimeout(30000);
        return dataSource;
    }
}

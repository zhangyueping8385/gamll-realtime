package org.example.realtime.view.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "doris")
public class DorisConfig {
    private String fenodes;
    private String database;
    private String username;
    private String password;
    private String driverClassName = "com.mysql.cj.jdbc.Driver";
    
    public String getUrl() {
        return "jdbc:mysql://" + fenodes + "/" + database + "?useSSL=false&characterEncoding=utf8";
    }
}
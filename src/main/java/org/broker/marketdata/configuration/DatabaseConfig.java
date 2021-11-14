package org.broker.marketdata.configuration;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@Setter
@ConfigurationProperties(prefix = "db.server")
@ToString
public class DatabaseConfig {
  @NonNull
  String host;
  @NonNull
  Integer port;
  @NonNull
  String database;
  @NonNull
  String user;
  @NonNull
  String password;
}

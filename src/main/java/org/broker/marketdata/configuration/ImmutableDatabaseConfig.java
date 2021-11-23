package org.broker.marketdata.configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

/*
  https://www.baeldung.com/configuration-properties-in-spring-boot
 */
@ConfigurationProperties(prefix = "db.server")
@ConstructorBinding
@AllArgsConstructor
@Getter
@ToString
public class ImmutableDatabaseConfig {
  @NonNull
  private final String host;
  @NonNull
  private final Integer port;
  @NonNull
  private final String database;
  @NonNull
  private final String user;
  @NonNull
  private final String password;
}

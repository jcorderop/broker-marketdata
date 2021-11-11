package org.broker.marketdata.configuration;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.broker.marketdata.configuration.ConfigurationLoader.*;

@Builder
@Value
@ToString
public class DatabaseConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(DatabaseConfiguration.class);

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

  public static DatabaseConfiguration from(final JsonObject config) {
    logger.info("Creating Server configuration...");
    final DocumentContext context = JsonPath.parse(config.encode());
    logger.info("Configuration to be loaded: {}", context.jsonString());
    return DatabaseConfiguration.builder()
      .host(context.read(PATH_DB_SERVER_HOST))
      .port(context.read(PATH_DB_SERVER_PORT))
      .database(context.read(PATH_DB_SERVER_DATABASE))
      .user(context.read(PATH_DB_SERVER_USER))
      .password(context.read(PATH_DB_SERVER_PASSWORD))
      .build();
  }

}

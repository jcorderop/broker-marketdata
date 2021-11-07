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
public class ServerConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(ServerConfiguration.class);

  @NonNull
  String host;
  @NonNull
  Integer port;
  @NonNull
  String path;

  public static ServerConfiguration from(final JsonObject config) {
    logger.info("Creating Server configuration...");
    final DocumentContext context = JsonPath.parse(config.encode());
    logger.info("Configuration to be loaded: {}", context.jsonString());
    return ServerConfiguration.builder()
      .host(context.read(PATH_WS_SERVER_HOST))
      .port(context.read(PATH_WS_SERVER_PORT))
      .path(context.read(PATH_WS_SERVER_PATH))
      .build();
  }
}

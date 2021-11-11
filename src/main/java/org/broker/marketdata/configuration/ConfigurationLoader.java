package org.broker.marketdata.configuration;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ConfigurationLoader {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);

  //database server configuration
  public static final String PATH_DB_SERVER_HOST = "$.db.server.host";
  public static final String PATH_DB_SERVER_PORT = "$.db.server.port";
  public static final String PATH_DB_SERVER_DATABASE = "$.db.server.database";
  public static final String PATH_DB_SERVER_USER = "$.db.server.user";
  public static final String PATH_DB_SERVER_PASSWORD = "$.db.server.password";
  //Websocket server configuration
  public static final String PATH_WS_SERVER_HOST = "$.ws.server.host";
  public static final String PATH_WS_SERVER_PORT = "$.ws.server.port";
  public static final String PATH_WS_SERVER_PATH = "$.ws.server.path";
  //Websocket client configuration
  public static final String PATH_WS_CLIENT_HOST = "$.ws.client.host";
  public static final String PATH_WS_CLIENT_PORT = "$.ws.client.port";
  public static final String PATH_WS_CLIENT_MAX_WEB_SOCKET_FRAME_SIZE = "$.ws.client.maxWebSocketFrameSize";
  public static final String PATH_WS_CLIENT_MAX_WEB_SOCKET_MESSAGE_SIZE = "$.ws.client.maxWebSocketMessageSize";
  public static final String PATH_WS_CLIENT_PATH = "$.ws.client.path";
  public static final String PATH_WS_CLIENT_SYMBOL = "$.ws.client.symbol";

  public static Future<ClientConfiguration> loadExchangeConfiguration(final Vertx vertx, ConfigurationFiles configFile) {
    return getConfigRetriever(vertx, configFile, ClientConfiguration.class.getName())
      .getConfig()
      .map(ClientConfiguration::from);
  }

  public static Future<ServerConfiguration> loadSeverConfiguration(final Vertx vertx, ConfigurationFiles configFile) {
    return getConfigRetriever(vertx, configFile, ServerConfiguration.class.getName())
      .getConfig()
      .map(ServerConfiguration::from);
  }

  public static Future<DatabaseConfiguration> loadDatabaseConfiguration(final Vertx vertx, ConfigurationFiles configFile) {
    return getConfigRetriever(vertx, configFile, DatabaseConfiguration.class.getName())
      .getConfig()
      .map(DatabaseConfiguration::from);
  }

  private static ConfigRetriever getConfigRetriever(final Vertx vertx, ConfigurationFiles configFile, String target) {
    logger.info("Fetching Configuration for service: {}", target);
    final var configRetrieverOptions = new ConfigRetrieverOptions();

    final var yamlStore = new ConfigStoreOptions()
      .setType("file")
      .setFormat("yaml")
      .setConfig(new JsonObject().put("path", configFile.getAppConfigurationFile()));
    configRetrieverOptions.addStore(yamlStore);

    // APP_ENV is set in docker
    Optional.ofNullable(System.getenv("APP_ENV"))
      .ifPresent(s -> {
        logger.info("Logging APP_ENV: {}", s);
        final var yamlStore_docker = new ConfigStoreOptions()
          .setType("file")
          .setFormat("yaml")
          .setConfig(new JsonObject().put("path", configFile.getDockerConfigurationFile()));
        configRetrieverOptions.addStore(yamlStore_docker);
      });

    logger.info("Creating configuration");
    return ConfigRetriever
      .create(vertx, configRetrieverOptions);
  }

  public static Handler<Throwable> getConfigurationThrowableHandler(Promise<Void> startPromise) {
    return throwable -> {
      logger.error("Could not load Database configuration, {}", throwable.getMessage());
      throwable.printStackTrace();
      throw new IllegalStateException(throwable);
    };
  }


}

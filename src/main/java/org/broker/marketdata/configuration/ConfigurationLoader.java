package org.broker.marketdata.configuration;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.module.ModuleFinder;

public class ConfigurationLoader {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);

  public static final String CONFIG_FILE = "application.yaml";

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
  //public static final String PATH_WS_CLIENT_QUOTE_SUBSCRIPTION_REQUEST = "$.ws.client.quoteSubscriptionRequest";
  //public static final String PATH_WS_CLIENT_ORDER_BOOK_L_2_25_SUBSCRIPTION_REQUEST = "$.ws.client.orderBookL2_25SubscriptionRequest";
  public static final String PATH_WS_CLIENT_SYMBOL = "$.ws.client.symbol";

  public static Future<ClientConfiguration> loadExchangeConfiguration(final Vertx vertx) {
    return getConfigRetriever(vertx, "ExchangeConfiguration")
      .getConfig()
      .map(ClientConfiguration::from);
  }

  public static Future<ServerConfiguration> loadSeverConfiguration(final Vertx vertx) {
    return getConfigRetriever(vertx, "ServerConfiguration")
      .getConfig()
      .map(ServerConfiguration::from);
  }

  public static Future<DatabaseConfiguration> loadDatabaseConfiguration(Vertx vertx) {
    return getConfigRetriever(vertx, "DataBaseConfiguration")
      .getConfig()
      .map(DatabaseConfiguration::from);
  }

  private static ConfigRetriever getConfigRetriever(final Vertx vertx, String target) {
    logger.info("Fetching {} configuration from: {}", target, CONFIG_FILE);
    final var yamlStore = new ConfigStoreOptions()
      .setType("file")
      .setFormat("yaml")
      .setConfig(new JsonObject().put("path", CONFIG_FILE));

    logger.info("Storing {} configuration", target);
    return ConfigRetriever
      .create(vertx, new ConfigRetrieverOptions()
      .addStore(yamlStore));
  }



}

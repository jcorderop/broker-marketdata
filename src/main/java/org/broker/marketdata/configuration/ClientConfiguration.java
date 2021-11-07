package org.broker.marketdata.configuration;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.broker.marketdata.configuration.ConfigurationLoader.*;

@Builder
@Value
@ToString
public class ClientConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(ClientConfiguration.class);

  @NonNull
  String host;
  @NonNull
  Integer port;
  @NonNull
  String path;
  @NonNull
  JsonObject subscription;

  Integer maxWebSocketFrameSize;
  Integer maxWebSocketMessageSize;

  public static ClientConfiguration from(final JsonObject config) {
    logger.info("Creating Exchange configuration...");
    final DocumentContext context = JsonPath.parse(config.encode());
    logger.info("Configuration to be loaded: {}", context.jsonString());

    return ClientConfiguration.builder()
      .host(context.read(PATH_WS_CLIENT_HOST))
      .port(context.read(PATH_WS_CLIENT_PORT))
      .path(buildSubscriptionPath(context))
      .maxWebSocketFrameSize(context.read(PATH_WS_CLIENT_MAX_WEB_SOCKET_FRAME_SIZE))
      .maxWebSocketMessageSize(context.read(PATH_WS_CLIENT_MAX_WEB_SOCKET_MESSAGE_SIZE))
      .subscription(buildSubscription(context))
      .build();
  }

  private static JsonObject buildSubscription (final DocumentContext context) {
    return new JsonObject().put("op", "subscribe").put("args", createSymbolSubscription(context));
  }

  private static JsonArray createSymbolSubscription(DocumentContext context) {
    logger.info("Loading symbol...");
    final JsonArray subscriptions = new JsonArray();
    ((List < String >) context.read(PATH_WS_CLIENT_SYMBOL)).stream()
      .map(ticker -> new StringBuilder()
        .append("instrument:")
        .append(ticker).toString())
      .forEach(subscription -> {
        logger.info(subscription);
        subscriptions.add(subscription);
      });
    logger.info("Subscription: {}", subscriptions.encode());
    return subscriptions;
  }

  private static String buildSubscriptionPath(final DocumentContext context) {
    logger.info("Building websocket subscription path");
    final String path = new StringBuilder()
      .append(context.read(PATH_WS_CLIENT_PATH).toString())
      //.append(context.read(PATH_WS_QUOTE_SUBSCRIPTION_REQUEST).toString())
      //.append(context.read(PATH_WS_CLIENT_ORDER_BOOK_L_2_25_SUBSCRIPTION_REQUEST).toString())
      .toString();
    logger.info("Subscription path: {}", path);
    return path;
  }
}

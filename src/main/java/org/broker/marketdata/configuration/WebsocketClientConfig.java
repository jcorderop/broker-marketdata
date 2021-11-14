package org.broker.marketdata.configuration;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@Getter @Setter
@ConfigurationProperties(prefix = "ws.client")
@ToString
public class WebsocketClientConfig {

  private static final Logger logger = LoggerFactory.getLogger(WebsocketClientConfig.class);

  @NonNull
  String host;
  @NonNull
  Integer port;
  @NonNull
  String path;

  List<String> symbol;

  @NonNull
  Integer maxWebSocketFrameSize;
  @NonNull
  Integer maxWebSocketMessageSize;

  public JsonObject buildSubscription () {
    return new JsonObject().put("op", "subscribe").put("args", createSymbolSubscription());
  }

  private JsonArray createSymbolSubscription() {
    logger.info("Loading symbol...");
    final JsonArray subscriptions = new JsonArray();
    symbol.stream()
      .map(symbol -> "instrument:" + symbol)
      .forEach(subscription -> {
        logger.debug(subscription);
        subscriptions.add(subscription);
      });
    logger.info("Subscription: {}", subscriptions.encode());
    return subscriptions;
  }
}

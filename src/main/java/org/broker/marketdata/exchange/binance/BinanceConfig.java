package org.broker.marketdata.exchange.binance;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.ToString;
import org.broker.marketdata.configuration.AbstractExchangeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "ws.binance.client")
@ConstructorBinding
@Getter
@ToString
public class BinanceConfig extends AbstractExchangeConfig {

  private static final Logger logger = LoggerFactory.getLogger(BinanceConfig.class);

  public static final String JSON_ACTION_UPDATE = "update";
  public static final String JSON_TOPIC_INSTRUMENT = "instrument";
  public static final String JSON_SYMBOL = "s";
  public static final String JSON_BID_PRICE = "b";
  public static final String JSON_ASK_PRICE = "a";

  @Override
  public JsonObject buildSubscription () {
    return new JsonObject().put("method", "SUBSCRIBE")
      .put("params", createSymbolSubscription())
      .put("id", 1);
  }

  private JsonArray createSymbolSubscription() {
    logger.info("Loading symbol...");
    final JsonArray subscriptions = new JsonArray();
    this.getSymbol().keySet().stream()
      .map(symbol -> symbol.toLowerCase() + "@bookTicker") //realtime
      .forEach(subscription -> {
        logger.debug(subscription);
        subscriptions.add(subscription);
      });
    logger.info("Subscription: {}", subscriptions.encode());
    return subscriptions;

  }
}

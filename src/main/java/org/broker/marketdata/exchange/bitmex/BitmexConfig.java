package org.broker.marketdata.exchange.bitmex;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import org.broker.marketdata.configuration.ExchangeConfig;
import org.broker.marketdata.configuration.ExchangeConfigImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "ws.bitmex.client")
public class BitmexConfig extends ExchangeConfigImpl implements ExchangeConfig {

  private static final Logger logger = LoggerFactory.getLogger(BitmexConfig.class);

  public static final Tuple JSON_VOLUME = Tuple.of("volume", "$.volume");
  public static final Tuple JSON_ASK_PRICE = Tuple.of("askPrice", "$.askPrice");
  public static final Tuple JSON_MID_PRICE = Tuple.of("midPrice", "$.midPrice");
  public static final Tuple JSON_BID_PRICE = Tuple.of("bidPrice", "$.bidPrice");
  public static final Tuple JSON_MARK_PRICE = Tuple.of("markPrice", "$.markPrice");
  public static final Tuple JSON_SYMBOL = Tuple.of("symbol", "$.symbol");
  public static final Tuple JSON_TABLE = Tuple.of("table", "$.table");
  public static final Tuple JSON_ACTION = Tuple.of("action", "$.action");
  public static final Tuple JSON_TIMESTAMP = Tuple.of("timestamp", "$.timestamp");


  public JsonObject buildSubscription () {
    return new JsonObject().put("op", "subscribe").put("args", createSymbolSubscription());
  }

  private JsonArray createSymbolSubscription() {
    logger.info("Loading symbol...");
    final JsonArray subscriptions = new JsonArray();
    this.getSymbol().stream()
      .map(symbol -> "instrument:" + symbol)
      .forEach(subscription -> {
        logger.debug(subscription);
        subscriptions.add(subscription);
      });
    logger.info("Subscription: {}", subscriptions.encode());
    return subscriptions;
  }
}

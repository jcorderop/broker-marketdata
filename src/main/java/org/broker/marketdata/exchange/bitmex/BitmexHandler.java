package org.broker.marketdata.exchange.bitmex;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class BitmexHandler implements ExchangeHandler {

  private static final Logger logger = LoggerFactory.getLogger(BitmexHandler.class);

  public static final String SOURCE = "BITMEX";

  public static final Tuple JSON_VOLUME = Tuple.of("volume", "$.volume");
  public static final Tuple JSON_ASK_PRICE = Tuple.of("askPrice", "$.askPrice");
  public static final Tuple JSON_MID_PRICE = Tuple.of("midPrice", "$.midPrice");
  public static final Tuple JSON_BID_PRICE = Tuple.of("bidPrice", "$.bidPrice");
  public static final Tuple JSON_MARK_PRICE = Tuple.of("markPrice", "$.markPrice");
  public static final Tuple JSON_SYMBOL = Tuple.of("symbol", "$.symbol");
  public static final Tuple JSON_TABLE = Tuple.of("table", "$.table");
  public static final Tuple JSON_ACTION = Tuple.of("action", "$.action");
  public static final Tuple JSON_TIMESTAMP = Tuple.of("timestamp", "$.timestamp");

  @Override
  public boolean isConnected(final Buffer buffer) {
    final var json = buffer.toJsonObject();
    logger.info("BitMEX Connection Initial Response: {}", buffer);
    try {
      logger.info("BitMEX Websocket Connected!");
      return true;
    } catch (Exception e) {
      logger.error("BitMEX Websocket Connected response is not as expected!");
      e.printStackTrace();
      return false;
    }
  }
}

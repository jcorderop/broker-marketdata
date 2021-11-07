package org.broker.marketdata.exchange.bitmex;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Tuple;
import org.broker.marketdata.client.WebSocketClientVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

  public static final List<Tuple> DATA_FIELDS = Arrays.asList(JSON_SYMBOL
      , JSON_MARK_PRICE
      , JSON_BID_PRICE
      , JSON_MID_PRICE
      , JSON_ASK_PRICE
      , JSON_VOLUME)
    .stream()
    .collect(Collectors.toUnmodifiableList());

  @Override
  public boolean isConnected(final Buffer buffer) {
    final var json = buffer.toJsonObject();
    logger.info("BitMEX Connection Initial Response: {}",buffer.toString());
    try {
      //Optional.of(json.getString("info"));
      //Optional.of(json.getString("version"));
      //Optional.of(json.getString("timestamp"));
      logger.info("BitMEX Websocket Connected!");
      return true;
    } catch (Exception e) {
      logger.error("BitMEX Websocket Connected response is not as expected!");
      e.printStackTrace();
      return false;
    }
  }
}

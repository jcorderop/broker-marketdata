package org.broker.marketdata.configuration;

import io.vertx.core.json.JsonObject;

import java.util.Map;

public interface ExchangeConfig {
  String getSource();

  String getHost();

  Integer getPort();

  String getPath();

  Map<String, String> getSymbol();

  Integer getMaxWebSocketFrameSize();

  Integer getMaxWebSocketMessageSize();

  JsonObject buildSubscription();
}

package org.broker.marketdata.configuration;

import io.vertx.core.json.JsonObject;

import java.util.List;

public interface ExchangeConfig {
  String getSource();
  String getHost();
  Integer getPort();
  String getPath();
  List<String> getSymbol();
  Integer getMaxWebSocketFrameSize();
  Integer getMaxWebSocketMessageSize();

  JsonObject buildSubscription ();
}

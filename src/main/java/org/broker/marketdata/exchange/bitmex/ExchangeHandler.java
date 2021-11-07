package org.broker.marketdata.exchange.bitmex;

import io.vertx.core.buffer.Buffer;

public interface ExchangeHandler {

  boolean isConnected(final Buffer buffer);
}

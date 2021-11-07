package org.broker.marketdata.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.client.WebSocketClientVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PriceBroadcast {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketHandler.class);
  private final Map<String, ServerWebSocket> connections = new HashMap<>();

  public void register(ServerWebSocket ws) {
    connections.put(ws.textHandlerID(), ws);
  }

  public void unregister(ServerWebSocket ws) {
    connections.remove(ws.textHandlerID());
  }

  public Map<String, ServerWebSocket> getConnections() {
    return Collections.unmodifiableMap(connections);
  }
}

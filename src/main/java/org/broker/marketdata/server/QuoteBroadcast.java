package org.broker.marketdata.server;

import io.vertx.core.http.ServerWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class QuoteBroadcast {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketHandler.class);
  private final Map<String, ServerWebSocket> connections = new HashMap<>();

  public void register(ServerWebSocket ws) {
    connections.put(ws.textHandlerID(), ws);
    logger.info("Client added, number of connections: {}", connections.size());
  }

  public void unregister(ServerWebSocket ws) {
    connections.remove(ws.textHandlerID());
    logger.info("Client removed, Number of connections: {}", connections.size());
  }

  public Map<String, ServerWebSocket> getConnections() {
    return Collections.unmodifiableMap(connections);
  }
}

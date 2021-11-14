package org.broker.marketdata.server;

import io.vertx.core.Handler;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebsocketHandler implements Handler<ServerWebSocket> {

  private static final Logger logger = LoggerFactory.getLogger(WebsocketHandler.class);

  public static final String DISCONNECT = "disconnect";
  public static final String CONNECTED = "connected";

  private final QuoteBroadcast quoteBroadcast;
  private final String path;

  public WebsocketHandler(String path, QuoteBroadcast quoteBroadcast) {
    this.quoteBroadcast = quoteBroadcast;
    this.path = path;
    logger.info("WebsocketHandler Server Has been constructed.");
  }

  @Override
  public void handle(ServerWebSocket ws) {
    if (isValidPath(ws)) return;

    logger.info("Opening web socket connection: {} - {}"
      , ws.path()
      , ws.textHandlerID());

    ws.accept();
    ws.frameHandler(frameHandler(ws));
    ws.endHandler(onCloseHandler(ws));
    ws.exceptionHandler(err -> logger.error("Client connection failed: ", err));

    quoteBroadcast.register(ws);
    ws.writeTextMessage(CONNECTED);
  }

  private Handler<Void> onCloseHandler(ServerWebSocket ws) {
    return onClose -> {
      logger.info("Connection closed: {}", ws.textHandlerID());
      quoteBroadcast.unregister(ws);
    };
  }

  private boolean isValidPath(ServerWebSocket ws) {
    if (!this.path.equalsIgnoreCase(ws.path())) {
      logger.warn("Connection rejected to this path: {}"
        , ws.path());
      ws.writeFinalTextFrame("Wrong path, only path " + this.path + " is accepted...");
      closeClient(ws);
      return true;
    }
    return false;
  }

  private Handler<WebSocketFrame> frameHandler(ServerWebSocket ws) {
    return received -> {
      final String message = received.textData();
      logger.info("Message Received: {} from client {}", message, ws.textHandlerID());

      if (DISCONNECT.equalsIgnoreCase(message)) {
        logger.info("Client close request!");
        closeClient(ws);
      } else {
        ws.writeTextMessage("Not Supported => (" + message + ")");
      }
    };
  }

  private void closeClient(ServerWebSocket ws) {
    ws.close((short)1000, "Normal Closure.");
  }
}

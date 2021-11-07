package org.broker.marketdata.common;

import io.vertx.core.Promise;
import io.vertx.core.http.WebSocket;
import org.slf4j.Logger;

public interface VerticleCommon {

  default void deforeStartVerticle(final Logger logger, final String className) {
    logger.info("Starting Verticle: {}", className);
  }

  default void completeVerticle(final Promise<Void> startPromise, final String className, final Logger logger) {
    startPromise.complete();
    logger.info("Verticle has been completed: {}", className);
  }

  default void websocketConnectionDetail(final WebSocket ws, final Logger logger) {
    logger.info("TextHandlerID: {}", ws.textHandlerID());
    logger.info("BinaryHandlerID: {}", ws.binaryHandlerID());
    logger.info("LocalAddress: {}", ws.localAddress());
    logger.info("RemoteAddress: {}", ws.remoteAddress());
    logger.info("SslSession: {}", ws.sslSession());
    logger.info("IsSsl: {}", ws.isSsl());
    logger.info("IsClosed: {}", ws.isClosed());
  }

}

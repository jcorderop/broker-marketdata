package org.broker.marketdata.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ConfigurationLoader;
import org.broker.marketdata.configuration.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSockerServerVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(WebSockerServerVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    ConfigurationLoader.loadSeverConfiguration(vertx)
      .onFailure(startPromise::fail)
      .onSuccess(serverConfig -> logger.info("Configuration successful loaded..."))
      .onComplete(serverConfig -> {
        logger.info("Retrieved Configuration {}", serverConfig);
        createWebSocketServer(startPromise, serverConfig.result());
      });
  }

  private void createWebSocketServer(Promise<Void> startPromise, ServerConfiguration serverConfig) {
    final Integer port = serverConfig.getPort();
    vertx.createHttpServer()
      .webSocketHandler(new WebSocketHandler(vertx, serverConfig.getPath()))
      .listen(port, http -> {
        if (http.succeeded()) {
          completeVerticle(startPromise, this.getClass().getName(), logger);
          logger.info("WebSocket Server started on port {}",port);
        } else {
          logger.error("WebSocket Server could not start, ", http.cause());
          startPromise.fail(http.cause());
        }
      });
  }

}

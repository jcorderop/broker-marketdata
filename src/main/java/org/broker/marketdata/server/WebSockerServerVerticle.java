package org.broker.marketdata.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ConfigurationFiles;
import org.broker.marketdata.configuration.ConfigurationLoader;
import org.broker.marketdata.configuration.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.broker.marketdata.configuration.ConfigurationLoader.getConfigurationThrowableHandler;

public class WebSockerServerVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(WebSockerServerVerticle.class);

  private final ConfigurationFiles configurationFiles;

  public WebSockerServerVerticle(ConfigurationFiles configurationFiles) {
    this.configurationFiles = configurationFiles;
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    ConfigurationLoader.loadSeverConfiguration(vertx, configurationFiles)
      .onFailure(getConfigurationThrowableHandler(startPromise))
      .onSuccess(serverConfig -> logger.info("Retrieved Configuration {}", serverConfig))
      .onComplete(serverConfig -> createWebSocketServer(startPromise, serverConfig.result()));
  }

  private void createWebSocketServer(final Promise<Void> startPromise, final ServerConfiguration serverConfig) {
    final Integer port = serverConfig.getPort();
    vertx.createHttpServer()
      .webSocketHandler(new WebSocketHandler(vertx, serverConfig.getPath()))
      .listen(port, http -> {
        if (http.succeeded()) {
          completeVerticle(startPromise, this.getClass().getName(), logger);
          logger.info("WebSocket Server started on port {}", port);
        } else {
          logger.error("WebSocket Server could not start, ", http.cause());
          startPromise.fail(http.cause());
        }
      });
  }

}

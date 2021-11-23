package org.broker.marketdata.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.WebsocketServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WebsockerServerVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(WebsockerServerVerticle.class);

  private final WebsocketServerConfig websocketServerConfig;
  private final QuoteBroadcast quoteBroadcast;

  private final ServerPublishQuoteVerticle subscriptionQuoteVerticle;
  private final WebsocketHandler webSocketHandler;

  @Autowired
  public WebsockerServerVerticle(final WebsocketServerConfig websocketServerConfig,
                                 final QuoteBroadcast quoteBroadcast) {
    this.quoteBroadcast = quoteBroadcast;
    this.websocketServerConfig = websocketServerConfig;
    logger.info("{} configuration fetched: {}", WebsocketServerConfig.class.getSimpleName(), websocketServerConfig);
    this.webSocketHandler = new WebsocketHandler(websocketServerConfig.getPath(), this.quoteBroadcast);
    this.subscriptionQuoteVerticle = new ServerPublishQuoteVerticle(this.quoteBroadcast);
    logger.info("ServerPublishQuoteVerticle Server Has been constructed.");
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    vertx.deployVerticle(subscriptionQuoteVerticle)
      .onFailure(subscriptionFailureHandler(startPromise))
      .onSuccess(event -> logger.info("Publish event quote has been started."))
      .compose(next -> createWebSocketServer(startPromise));
  }

  private Handler<Throwable> subscriptionFailureHandler(final Promise<Void> startPromise) {
    return event -> {
      logger.error("Publish event quote could not start, {}", event.getMessage());
      startPromise.fail(event);
    };
  }

  private Future<Void>  createWebSocketServer(final Promise<Void> startPromise) {
    vertx.createHttpServer()
      .webSocketHandler(webSocketHandler)
      .listen(websocketServerConfig.getPort(), http -> {
        if (http.succeeded()) {
          completeVerticle(startPromise, this.getClass().getName(), logger);
          logger.info("WebSocket Server started on port {}", websocketServerConfig.getPort());
        } else {
          logger.error("WebSocket Server could not start, using port {}, {}", websocketServerConfig.getPort(), http.cause().getMessage());
          http.cause().printStackTrace();
          startPromise.fail(http.cause());
        }
      });
    return Future.succeededFuture();
  }
}

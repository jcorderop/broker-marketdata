package org.broker.marketdata;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.client.WebSocketClientVerticle;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.logging.LoggingVerticle;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.server.WebsockerServerVerticle;
import org.broker.marketdata.storage.StorageVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MarketDataService extends AbstractVerticle implements VerticleCommon {

  public static final Logger logger = LoggerFactory.getLogger(MarketDataService.class);

  @Autowired
  public WebSocketClientVerticle webSocketClientVerticle;

  @Autowired
  public WebsockerServerVerticle webSockerServerVerticle;

  @Autowired
  public LoggingVerticle loggingVerticle;

  @Autowired
  public StorageVerticle storageVerticle;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    vertx.exceptionHandler(getThrowableHandler());
    //ecodec used to send java objects
    vertx.eventBus().registerDefaultCodec(Quote.class, new LocalMessageCodec<>(Quote.class));

    startServices(startPromise);
  }


  private void startServices(Promise<Void> startPromise) {
    logger.info("Starting Market Data Service BITMEX Adapter...");
    vertx.deployVerticle(storageVerticle)
      .onFailure(startPromise::fail)
      .compose(next -> vertx.deployVerticle(loggingVerticle))
      .onFailure(startPromise::fail)
      .compose(next -> vertx.deployVerticle(webSockerServerVerticle))
      .onFailure(startPromise::fail)
      .compose(next -> {
        vertx.deployVerticle(webSocketClientVerticle);
        completeVerticle(startPromise, this.getClass().getName(), logger);
        return Future.succeededFuture();
      })
      .onFailure(startPromise::fail);
  }


  private Handler<Throwable> getThrowableHandler() {
    return event -> {
      // what to do with the uncaught exception
      logger.error("Unhandled exception by the vertx instances, {}", event.getMessage());
      event.printStackTrace();
    };
  }
}



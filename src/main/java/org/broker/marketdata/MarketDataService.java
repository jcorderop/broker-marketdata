package org.broker.marketdata;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.client.WebSocketClientVerticle;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ConfigurationFiles;
import org.broker.marketdata.configuration.DefaultConfigurationFiles;
import org.broker.marketdata.exchange.bitmex.BitmexHandler;
import org.broker.marketdata.logging.LoggingVerticle;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.server.WebSockerServerVerticle;
import org.broker.marketdata.storage.StorageVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarketDataService extends AbstractVerticle implements VerticleCommon {

  public static final Logger logger = LoggerFactory.getLogger(MarketDataService.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    vertx.exceptionHandler(getThrowableHandler());
    //ecodec used to send java objects
    vertx.eventBus().registerDefaultCodec(Quote.class, new LocalMessageCodec<>(Quote.class));

    startServices(startPromise);
  }

  private Handler<Throwable> getThrowableHandler() {
    return event -> {
      // what to do with the uncaught exception
      logger.error("Unhandled exception by the vertx instances, {}", event.getMessage());
      event.printStackTrace();
    };
  }

  private void startServices(Promise<Void> startPromise) {
    logger.info("Starting Market Data Service BITMEX Adapter...");
    ConfigurationFiles configurationFiles = new DefaultConfigurationFiles();

    // TODO - Use a framework such as Spring Boot or Quarkus on top to inject
    vertx.deployVerticle(new StorageVerticle(configurationFiles))
      .onFailure(startPromise::fail)
      .compose(next -> vertx.deployVerticle(new LoggingVerticle(configurationFiles)))
      .onFailure(startPromise::fail)
      .compose(next -> vertx.deployVerticle(new WebSockerServerVerticle(configurationFiles)))
      .onFailure(startPromise::fail)
      .compose(next -> {
        vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), configurationFiles));
        completeVerticle(startPromise, this.getClass().getName(), logger);
        return Future.succeededFuture();
      })
      .onFailure(startPromise::fail);

  }


}



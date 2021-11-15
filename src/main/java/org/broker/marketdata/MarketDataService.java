package org.broker.marketdata;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.RequiredArgsConstructor;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.exchange.binance.BinanceAdapter;
import org.broker.marketdata.exchange.bitmex.BitmexAdapter;
import org.broker.marketdata.logging.LoggingVerticle;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.server.WebsockerServerVerticle;
import org.broker.marketdata.storage.StorageVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MarketDataService extends AbstractVerticle implements VerticleCommon {

  public static final Logger logger = LoggerFactory.getLogger(MarketDataService.class);

  public final WebsockerServerVerticle webSockerServerVerticle;
  public final LoggingVerticle loggingVerticle;
  public final StorageVerticle storageVerticle;
  public final BitmexAdapter bitmexAdapter;
  public final BinanceAdapter binanceAdapter;

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
      .compose(next -> vertx.deployVerticle(bitmexAdapter))
      .onFailure(startPromise::fail)
      .compose(next -> vertx.deployVerticle(binanceAdapter))
      .onFailure(startPromise::fail)
      .onSuccess(event -> completeVerticle(startPromise, this.getClass().getName(), logger));
  }

  private Handler<Throwable> getThrowableHandler() {
    return event -> {
      // what to do with the uncaught exception
      logger.error("Unhandled exception by the vertx instances, {}", event.getMessage());
      event.printStackTrace();
    };
  }
}



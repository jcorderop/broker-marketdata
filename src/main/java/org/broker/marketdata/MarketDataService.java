package org.broker.marketdata;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.client.WebSocketClientVerticle;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.exchange.bitmex.BitmexHandler;
import org.broker.marketdata.logging.LoggingVerticle;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.server.WebSockerServerVerticle;
import org.broker.marketdata.storage.StorageVerticle;
import org.broker.marketdata.storage.db.DBPools;
import org.broker.marketdata.storage.db.migration.FlywayMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.broker.marketdata.configuration.ConfigurationLoader.loadDatabaseConfiguration;


public class MarketDataService extends AbstractVerticle implements VerticleCommon {

  public static final Logger logger = LoggerFactory.getLogger(MarketDataService.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    vertx.exceptionHandler(error -> {
      logger.error("Unhandled exception {}", error);
    });
    //used to send java objects
    vertx.eventBus().registerDefaultCodec(Quote.class, new LocalMessageCodec<>(Quote.class));

    startServices(startPromise);
  }

  private void startServices(Promise<Void> startPromise) {
    logger.info("Starting Market Data Service BITMEX Adapter...");

    vertx.deployVerticle(new StorageVerticle())
      .onFailure(startPromise::fail)
      .compose(next -> vertx.deployVerticle(new LoggingVerticle()))
      .onFailure(startPromise::fail)
      .compose(next -> vertx.deployVerticle(new WebSockerServerVerticle()))
      .onFailure(startPromise::fail)
      // TODO - Use a framework such as Spring Boot or Quarkus on top to inject
      .compose(next -> {
        vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler()));
        completeVerticle(startPromise, this.getClass().getName(), logger);
        return Future.succeededFuture();
      })
      .onFailure(startPromise::fail);

  }


}



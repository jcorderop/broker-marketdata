package org.broker.marketdata.storage;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ImmutableDatabaseConfig;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.protos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StorageVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(StorageVerticle.class);

  private final ImmutableDatabaseConfig immutableDatabaseConfig;

  @Autowired
  private StorageService storageService;

  @Autowired
  public StorageVerticle(ImmutableDatabaseConfig immutableDatabaseConfig) {
    this.immutableDatabaseConfig = immutableDatabaseConfig;
    logger.info("{} configuration fetched: {}", ImmutableDatabaseConfig.class.getSimpleName(), immutableDatabaseConfig);
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    createBusConsumerEvent(startPromise)
      .onFailure(startPromise::fail)
      .onComplete(event -> {
        if (event.succeeded()) {
          completeVerticle(startPromise, this.getClass().getName(), logger);
        } else {
          startPromise.fail("Verticle could not start.");
        }
      });

  }

  private Future<Void> createBusConsumerEvent(Promise<Void> startPromise) {
    logger.info("Creating consumer Vertx Bus, topic: {}", Topics.TOPIC_INTERNAL_QUOTE);
    vertx.eventBus()
      .consumer(Topics.TOPIC_STORAGE, this::insertNewQuote)
      .exceptionHandler(event -> {
        logger.error("Could not insert the a new Quote, {}", event.getMessage());
        throw new IllegalStateException(event);
      });
    return Future.succeededFuture();
  }

  private void insertNewQuote(Message<Quote> quote) {
    storageService.insertNewQuote(quote.body());
  }
}

package org.broker.marketdata.exchange.binance;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.broker.marketdata.AbstractVerticleSpringBootTest;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.protos.Quote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;


class BinanceAdapterSuccessTest extends AbstractVerticleSpringBootTest {

  private static final int EXPECTED_MESSAGES = 5;
  private static final Logger logger = LoggerFactory.getLogger(BinanceAdapterSuccessTest.class);

  @Autowired
  public BinanceAdapter binanceAdapter;

  AtomicInteger counter;

  @BeforeEach
  public void beforeEach() {
    counter = new AtomicInteger(0);
  }

  @Test
  void create_successfully_a_websocket_client_publishing_raw_quotes(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    vertx.eventBus().registerDefaultCodec(Quote.class, new LocalMessageCodec<>(Quote.class));

    //when
    Future<String> stringFuture = vertx.deployVerticle(binanceAdapter)
      .onComplete(event -> vertx.eventBus().<String>consumer(Topics.TOPIC_RAW_MESSAGE, quote -> {
        logger.info("Unit Test, New Message Received: {}", quote.body());
        var currentValue = counter.getAndIncrement();
        if (currentValue >= EXPECTED_MESSAGES) {
          testContext.completeNow();
        } else {
          logger.info("Unit Test, not enough messages yet... ({}/{})", currentValue, EXPECTED_MESSAGES);
        }
      }));

    vertx.exceptionHandler(getThrowableHandler(testContext));

    //then
    assertTrue(testContext.awaitCompletion(15, TimeUnit.SECONDS));
  }

  private Handler<Throwable> getThrowableHandler(VertxTestContext testContext) {
    return event -> {
      var currentValue = counter.getAndIncrement();
      if (!(currentValue >= EXPECTED_MESSAGES)) {
        logger.info("Could NOT connect!!!");
        logger.info("Unit Test: {}", event.getMessage());
      }
    };
  }
}

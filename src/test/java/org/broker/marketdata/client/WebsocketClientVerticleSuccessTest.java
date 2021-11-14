package org.broker.marketdata.client;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.configuration.WebsocketClientConfig;
import org.broker.marketdata.exchange.bitmex.BitmexHandler;
import org.broker.marketdata.protos.Quote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties="springBootApp.workOffline=true")
@ActiveProfiles("test")
@ExtendWith(VertxExtension.class)
class WebsocketClientVerticleSuccessTest {

  private static final int EXPECTED_MESSAGES = 5;
  private static final Logger logger = LoggerFactory.getLogger(WebsocketClientVerticleSuccessTest.class);

  @Autowired
  WebsocketClientConfig websocketClientConfig;

  @BeforeEach
  public void beforeEach() {

  }

  @Test
  void create_successfully_a_websocket_client_publishing_internal_quotes(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    vertx.eventBus().registerDefaultCodec(Quote.class, new LocalMessageCodec<>(Quote.class));
    final AtomicInteger counter = new AtomicInteger(0);

    //when
    vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), websocketClientConfig))
      .onComplete(event -> vertx.eventBus().<Quote>consumer(Topics.TOPIC_INTERNAL_QUOTE, quote -> {
        logger.info("Unit Test, New Message Received.");
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

  @Test
  void create_successfully_a_websocket_client_publishing_raw_quotes(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    final AtomicInteger counter = new AtomicInteger(0);

    //when
    Future<String> stringFuture = vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), websocketClientConfig))
      .onComplete(event -> vertx.eventBus().<String>consumer(Topics.TOPIC_RAW_MESSAGE, quote -> {
        logger.info("Unit Test, New Message Received.");
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
      logger.info("Could NOT connect!!!");
      logger.info("Unit Test: {}", event.getMessage());
      testContext.failed();
    };
  }
}


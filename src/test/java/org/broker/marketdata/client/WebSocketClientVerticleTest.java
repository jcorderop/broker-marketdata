package org.broker.marketdata.client;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.exchange.bitmex.BitmexHandler;
import org.broker.marketdata.protos.Quote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
class WebSocketClientVerticleTest {

  public static final int EXPECTED_MESSAGES = 5;
  private static final Logger logger = LoggerFactory.getLogger(WebSocketClientVerticleTest.class);

  @BeforeEach
  public void beforeEach() {

  }

  @Test
  void create_successfully_a_websocket_client_publishing_internal_quotes(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    final TestConfigurationFiles configurationFiles = commonWebsocketClientConfig(vertx, "application.yaml");
    final AtomicInteger counter = new AtomicInteger(0);

    //when
    vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), configurationFiles))
      .onComplete(event -> vertx.eventBus().<Quote>consumer(Topics.TOPIC_INTERNAL_QUOTE, quote -> {
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
    final TestConfigurationFiles configurationFiles = commonWebsocketClientConfig(vertx, "application.yaml");
    final AtomicInteger counter = new AtomicInteger(0);

    //when
    Future<String> stringFuture = vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), configurationFiles))
      .onComplete(event -> vertx.eventBus().<Quote>consumer(Topics.TOPIC_RAW_MESSAGE, quote -> {
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
  void wrong_url_could_not_create_websocket_client(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    final TestConfigurationFiles configurationFiles = new TestConfigurationFiles();
    configurationFiles.setAppFile("org/broker/marketdata/client/application.test.yaml");

    //when
    vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), configurationFiles));

    //then
    vertx.exceptionHandler(getHandlerForException(testContext));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  @Test
  void no_config_could_not_create_websocket_client(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    final TestConfigurationFiles configurationFiles = new TestConfigurationFiles();
    configurationFiles.setAppFile("");

    //when
    vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), configurationFiles));

    //then
    vertx.exceptionHandler(getHandlerForException(testContext));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  private TestConfigurationFiles commonWebsocketClientConfig(Vertx vertx, String configFile) {
    final TestConfigurationFiles configurationFiles = new TestConfigurationFiles();
    configurationFiles.setAppFile(configFile);
    //ecodec used to send java objects
    vertx.eventBus().registerDefaultCodec(Quote.class, new LocalMessageCodec<>(Quote.class));
    return configurationFiles;
  }

  private Handler<Throwable> getHandlerForException(VertxTestContext testContext) {
    return event -> {
      logger.info("Could NOT connect!!!");
      logger.info("Unit Test: {}", event.getMessage());
      assertThat(event).isInstanceOf(IllegalStateException.class);
      testContext.completeNow();
    };
  }

  private Handler<Throwable> getThrowableHandler(VertxTestContext testContext) {
    return event -> {
      logger.info("Could NOT connect!!!");
      logger.info("Unit Test: {}", event.getMessage());
      testContext.failed();
    };
  }
}


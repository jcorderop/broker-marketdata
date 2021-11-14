package org.broker.marketdata.client;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.broker.marketdata.configuration.WebsocketClientConfig;
import org.broker.marketdata.exchange.bitmex.BitmexHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties="springBootApp.workOffline=true")
@TestPropertySource(properties = {"spring.config.location = classpath:application-fail.yaml"})
@ExtendWith(VertxExtension.class)
class WebsocketClientVerticleTest {

  private static final Logger logger = LoggerFactory.getLogger(WebsocketClientVerticleTest.class);

  @Autowired
  WebsocketClientConfig websocketClientConfig;

  @BeforeEach
  public void beforeEach() {

  }

  @Test
  void wrong_url_could_not_create_websocket_client(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    //classpath:application-test.yaml with wrong config

    //when
    vertx.deployVerticle(new WebSocketClientVerticle(new BitmexHandler(), websocketClientConfig));

    //then
    vertx.exceptionHandler(getHandlerForException(testContext));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }


  private Handler<Throwable> getHandlerForException(VertxTestContext testContext) {
    return event -> {
      logger.info("Could NOT connect!!!");
      logger.info("Unit Test: {}", event.getMessage());
      assertThat(event).isInstanceOf(IllegalStateException.class);
      testContext.completeNow();
    };
  }
}


package org.broker.marketdata.exchange.binance;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
class BinanceAdapterFailTest {

  private static final Logger logger = LoggerFactory.getLogger(BinanceAdapterFailTest.class);

  @Autowired
  public BinanceAdapter binanceAdapter;

  @BeforeEach
  public void beforeEach() {

  }

  @Test
  void wrong_url_could_not_create_websocket_client(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    //classpath:application-fail.yaml with wrong config

    //when
    vertx.deployVerticle(binanceAdapter);

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


package org.broker.marketdata.server;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.broker.marketdata.AbstractVerticleSpringBootTest;
import org.broker.marketdata.configuration.WebsocketServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class WebsockerServerVerticleTest extends AbstractVerticleSpringBootTest {

  @Autowired
  WebsocketServerConfig webSockerServerVerticle;

  @Autowired
  QuoteBroadcast quoteBroadcast;

  @AfterEach
  void tearDown() {
  }

  @Test
  void create_successfully_server_vertx(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given

    //when
    vertx.deployVerticle(new WebsockerServerVerticle(webSockerServerVerticle, quoteBroadcast), testContext.succeeding(id -> testContext.completeNow()));

    //then
    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }
}

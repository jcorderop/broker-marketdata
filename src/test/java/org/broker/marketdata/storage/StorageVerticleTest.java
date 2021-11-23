package org.broker.marketdata.storage;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.broker.marketdata.AbstractVerticleSpringBootTest;
import org.broker.marketdata.configuration.ImmutableDatabaseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageVerticleTest extends AbstractVerticleSpringBootTest {

  @Autowired
  ImmutableDatabaseConfig immutableDatabaseConfig;

  @AfterEach
  void tearDown() {
  }

  @Test
  void create_successfully_storage_vertx(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given

    //when
    vertx.deployVerticle(new StorageVerticle(immutableDatabaseConfig), testContext.succeeding(id -> testContext.completeNow()));

    //then
    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }
}

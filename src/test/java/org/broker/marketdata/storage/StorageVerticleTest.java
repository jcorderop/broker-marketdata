package org.broker.marketdata.storage;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.broker.marketdata.configuration.DatabaseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties="springBootApp.workOffline=true")
@ActiveProfiles("test")
@ExtendWith(VertxExtension.class)
class StorageVerticleTest {

  @Autowired
  DatabaseConfig databaseConfig;

  @AfterEach
  void tearDown() {
  }

  @Test
  void create_successfully_storage_vertx(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given

    //when
    vertx.deployVerticle(new StorageVerticle(databaseConfig), testContext.succeeding(id -> testContext.completeNow()));

    //then
    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }
}

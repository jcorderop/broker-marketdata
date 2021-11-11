package org.broker.marketdata;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.broker.marketdata.server.WebSocketHandler.CONNECTED;
import static org.broker.marketdata.server.WebSocketHandler.DISCONNECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
class MarketDataServiceTest {

  public static final int EXPECTED_MESSAGES = 5;
  public static final int PORT = 8900;
  public static final String LOCALHOST = "localhost";
  public static final String REALTIME_QUOTES = "/realtime/quotes";
  private static final Logger logger = LoggerFactory.getLogger(MarketDataServiceTest.class);

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    vertx.deployVerticle(new MarketDataService(), testContext.succeeding(id -> testContext.completeNow()));
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void create_successfully_a_websocket_client_can_connect(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    var client = vertx.createHttpClient();

    //when
    client.webSocket(PORT, LOCALHOST, REALTIME_QUOTES)
      .onFailure(testContext::failNow)
      .onComplete(testContext.succeeding(ws -> ws.handler(data -> {
          final var receivedData = data.toString();
          logger.info("Unit Test, Received message: {}", receivedData);
          assertEquals(CONNECTED, receivedData);
          closeConnection(testContext, client);
        }))
      );

    //then
    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  @Test
  void create_successfully_a_websocket_client_can_disconnect(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    var client = vertx.createHttpClient();

    //when
    client.webSocket(PORT, LOCALHOST, REALTIME_QUOTES)
      .onFailure(testContext::failNow)
      .onComplete(testContext.succeeding(ws -> {
          ws.handler(data -> {
            final var receivedData = data.toString();
            logger.info("Received message: {}", receivedData);
          });

          ws.writeTextMessage(DISCONNECT).onComplete(event -> closeConnection(testContext, client));
        })
      );

    //then
    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  @Test
  void create_successfully_a_websocket_client_wrong_path(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    var client = vertx.createHttpClient();

    //when
    client.webSocket(PORT, LOCALHOST, REALTIME_QUOTES + "+")
      .onFailure(testContext::failNow)
      .onComplete(testContext.succeeding(ws -> ws.handler(data -> {
          final var receivedData = data.toString();
          logger.info("Unit Test, Received message: {}", receivedData);
          assertThat(receivedData.toUpperCase()).contains("WRONG");
          closeConnection(testContext, client);
        }))
      );

    //then
    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  @Test
  void create_successfully_a_websocket_client_publishing_to_read_quotes(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    var client = vertx.createHttpClient();
    final AtomicInteger counter = new AtomicInteger(0);

    //when
    client.webSocket(new WebSocketConnectOptions()
        .setHost(LOCALHOST)
        .setPort(PORT)
        .setURI(REALTIME_QUOTES)
      )
      .onFailure(testContext::failNow)
      .onComplete(testContext.succeeding(ws -> ws.handler(data -> {
          final var receivedData = data.toString();
          logger.info("Received message: {}", receivedData);
          var currentValue = counter.getAndIncrement();
          if (currentValue >= EXPECTED_MESSAGES) {
            client.close();
            testContext.completeNow();
          } else {
            logger.info("Unit Test, not enough messages yet... ({}/{})", currentValue, EXPECTED_MESSAGES);
          }
        }))
      );

    //then
    assertTrue(testContext.awaitCompletion(15, TimeUnit.SECONDS));
  }

  private void closeConnection(VertxTestContext testContext, HttpClient client) {
    try {
      TimeUnit.SECONDS.sleep(2);
      client.close();
      testContext.completeNow();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

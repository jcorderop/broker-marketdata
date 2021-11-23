package org.broker.marketdata;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.broker.marketdata.server.WebsocketHandler.CONNECTED;
import static org.broker.marketdata.server.WebsocketHandler.DISCONNECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketDataServiceTest extends AbstractVerticleSpringBootTest {

  private static final int EXPECTED_MESSAGES = 5;
  private static final int PORT = 8900;
  private static final String LOCALHOST = "localhost";
  private static final String REALTIME_QUOTES = "/realtime/quotes";

  private static final Logger logger = LoggerFactory.getLogger(MarketDataServiceTest.class);

  @Autowired
  MarketDataService marketDataService;

  @BeforeEach
  void deploy_vertible(Vertx vertx, VertxTestContext testContext) {
    vertx.deployVerticle(marketDataService, testContext.succeeding(id -> testContext.completeNow()));
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void create_successfully_a_websocket_client_can_connect(Vertx vertx, VertxTestContext testContext) throws Throwable {
    //given
    var client = vertx.createHttpClient();
    AtomicBoolean connected = new AtomicBoolean(false);

    //when
    client.webSocket(PORT, LOCALHOST, REALTIME_QUOTES)
      .onFailure(testContext::failNow)
      .onComplete(testContext.succeeding(ws -> ws.handler(data -> {
          final var receivedData = data.toString();
          logger.info("Unit Test, Received message: {}", receivedData);
          if (!connected.get()) {
            assertEquals(CONNECTED, receivedData);
            connected.set(true);
            closeConnection(testContext, client);
          }
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
          logger.info("Unit Test, Received message: {}", receivedData);
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

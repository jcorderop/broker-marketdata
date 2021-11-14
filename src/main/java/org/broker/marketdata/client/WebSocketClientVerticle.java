package org.broker.marketdata.client;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.configuration.WebsocketClientConfig;
import org.broker.marketdata.exchange.bitmex.ExchangeHandler;
import org.broker.marketdata.protos.normalizer.QuoteNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Component
public class WebSocketClientVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketClientVerticle.class);

  private final ExchangeHandler exchangeHandler;
  private final WebsocketClientConfig websocketClientConfig;

  @Autowired
  public WebSocketClientVerticle(ExchangeHandler exchangeHandler
    , WebsocketClientConfig websocketClientConfig) {
    this.exchangeHandler = exchangeHandler;
    this.websocketClientConfig = websocketClientConfig;
    logger.info("{} configuration fetched: {}", WebsocketClientConfig.class.getSimpleName(), websocketClientConfig);
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    createWebSocketClient(startPromise);
  }

  private void createWebSocketClient(Promise<Void> startPromise) {
    logger.info("Creating WebSocket Client...");

    Future<WebSocket> webSocketFuture = getHttpClient()
      .webSocket(getWebSocketConfiguration());

    webSocketFuture
      .onFailure(throwable -> logger.error(throwable.getMessage()))
      .compose(this::isConnect)
      .onFailure(throwable -> logger.error(throwable.getMessage()))
      .compose(subscribe())
      .onFailure(throwable -> logger.error(throwable.getMessage()))
      .onComplete(eventHandler(startPromise));
  }

  private HttpClient getHttpClient() {
    return vertx.createHttpClient(getHttpClientOptions());
  }

  private HttpClientOptions getHttpClientOptions() {
    return new HttpClientOptions()
      .setMaxWebSocketFrameSize(websocketClientConfig.getMaxWebSocketFrameSize())
      .setMaxWebSocketMessageSize(websocketClientConfig.getMaxWebSocketMessageSize());
  }

  private Handler<AsyncResult<WebSocket>> eventHandler(Promise<Void> startPromise) {
    return webSocketAsyncResult -> {
      if (webSocketAsyncResult.succeeded()) {
        webSocketAsyncResult.result().handler(onEvent());
        // Till here the vertx is complete
        completeVerticle(startPromise, this.getClass().getName(), logger);
      } else if (webSocketAsyncResult.failed()) {
        logger.error("Failure during connection...");
        webSocketAsyncResult.cause().printStackTrace();
        throw new IllegalStateException("Something went wrong during the websocket connection, websocket client could not be initialized.");
      }
    };
  }

  private Handler<Buffer> onEvent() {
    return buffer -> {
      // Used to log raw price
      vertx.eventBus().publish(Topics.TOPIC_RAW_MESSAGE, buffer.toString());
      // Used as internal quote
      QuoteNormalizer.stringToQuote(buffer.toString())
        .forEach(quote -> vertx.eventBus().publish(Topics.TOPIC_INTERNAL_QUOTE, quote));
    };
  }

  private Function<WebSocket, Future<WebSocket>> subscribe() {
    return webSocket -> {
      final JsonObject subcription = websocketClientConfig.buildSubscription();
      logger.info("Subscribing to: {}", subcription.encode());
      webSocket.writeTextMessage(subcription.toString());
      return Future.succeededFuture(webSocket);
    };
  }

  private Future<WebSocket> isConnect(WebSocket webSocket) {
    if (!webSocket.isClosed()) {
      logger.info("Connected!");
    } else {
      logger.error("Connection Failed!");
    }
    return Future.succeededFuture(webSocket);
  }

  private WebSocketConnectOptions getWebSocketConfiguration() {
    return new WebSocketConnectOptions()
      .setHost(websocketClientConfig.getHost())
      .setPort(websocketClientConfig.getPort())
      .setURI(websocketClientConfig.getPath())
      .setSsl(true);
  }
}

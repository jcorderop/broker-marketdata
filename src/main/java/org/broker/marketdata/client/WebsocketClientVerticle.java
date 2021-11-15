package org.broker.marketdata.client;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ExchangeConfig;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.exchange.bitmex.BitmexConfig;
import org.broker.marketdata.protos.normilizer.QuoteNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class WebsocketClientVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(WebsocketClientVerticle.class);

  private final ExchangeConfig exchangeConfig;
  private final QuoteNormalizer quoteNormalizer;

  public WebsocketClientVerticle(ExchangeConfig exchangeConfig, QuoteNormalizer quoteNormalizer) {
    this.exchangeConfig = exchangeConfig;
    this.quoteNormalizer = quoteNormalizer;
    logger.info("{} configuration fetched: {}", BitmexConfig.class.getSimpleName(), exchangeConfig);
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

    logger.info("webSocketFuture...");
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
      .setMaxWebSocketFrameSize(exchangeConfig.getMaxWebSocketFrameSize())
      .setMaxWebSocketMessageSize(exchangeConfig.getMaxWebSocketMessageSize());
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
      quoteNormalizer.stringToQuote(buffer.toString())
        .forEach(quote -> vertx.eventBus().publish(Topics.TOPIC_INTERNAL_QUOTE, quote));
    };
  }

  private Function<WebSocket, Future<WebSocket>> subscribe() {
    return webSocket -> {
      final JsonObject subcription = exchangeConfig.buildSubscription();
      logger.info("Subscribing to: {}", subcription.encode());
      webSocket.writeTextMessage(subcription.toString());
      return Future.succeededFuture(webSocket);
    };
  }

  private Future<WebSocket> isConnect(WebSocket webSocket) {
    System.out.println("isConnect");
    if (!webSocket.isClosed()) {
      logger.info("Connected!");
    } else {
      logger.error("Connection Failed!");
    }
    return Future.succeededFuture(webSocket);
  }

  private WebSocketConnectOptions getWebSocketConfiguration() {
    return new WebSocketConnectOptions()
      .setHost(exchangeConfig.getHost())
      .setPort(exchangeConfig.getPort())
      .setURI(exchangeConfig.getPath())
      .setSsl(true);
  }
}

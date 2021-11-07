package org.broker.marketdata.client;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ConfigurationLoader;
import org.broker.marketdata.configuration.ClientConfiguration;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.exchange.bitmex.ExchangeHandler;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.protos.normalizer.QuoteNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class WebSocketClientVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketClientVerticle.class);

  private final ExchangeHandler exchangeHandler;

  public WebSocketClientVerticle(ExchangeHandler exchangeHandler) {
    this.exchangeHandler = exchangeHandler;
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    ConfigurationLoader.loadExchangeConfiguration(vertx)
      .onFailure(startPromise::fail)
      .onSuccess(config -> logger.info("Configuration successful loaded..."))
      .onComplete(startWebsocketClient(startPromise));
  }

  private Handler<AsyncResult<ClientConfiguration>> startWebsocketClient(Promise<Void> startPromise) {
    return configLoaded -> {
      logger.info("Retrieved Configuration {}", configLoaded);
      if (configLoaded.succeeded()) {
        ClientConfiguration config = configLoaded.result();
        final HttpClient httpClient = vertx.createHttpClient(getHttpClientOptions(config));
        createWebSocketClient(startPromise, config, httpClient);
        completeVerticle(startPromise, this.getClass().getName(), logger);
      } else if (configLoaded.failed()) {
        logger.error("Failure loading client configuration...");
        configLoaded.cause().printStackTrace();
        throw new IllegalStateException("Something went wrong loading configuration.");
      }
    };
  }

  private static HttpClientOptions getHttpClientOptions(ClientConfiguration config) {
    return new HttpClientOptions()
      .setMaxWebSocketFrameSize(config.getMaxWebSocketFrameSize())
      .setMaxWebSocketMessageSize(config.getMaxWebSocketMessageSize())
    ;
  }

  private void createWebSocketClient(Promise<Void> startPromise, ClientConfiguration config, HttpClient httpClient) {
    logger.info("Creating WebSocket Client...");
    Future<WebSocket> webSocketFuture = httpClient.webSocket(getWebSocketConfiguration(config));

    webSocketFuture
      .onFailure(throwable -> logger.error(throwable.getMessage()))
      .compose(this::isConnect)
      .onFailure(throwable -> logger.error(throwable.getMessage()))
      .compose(subscribe(config))
      .onFailure(throwable -> logger.error(throwable.getMessage()))
      .onComplete(eventHandler());
  }

  private Handler<AsyncResult<WebSocket>> eventHandler() {
    return webSocketAsyncResult -> {
      if (webSocketAsyncResult.succeeded()) {
        webSocketAsyncResult.result().handler(onEvent());
      } else if (webSocketAsyncResult.failed()) {
        logger.error("Failure during connection...");
        webSocketAsyncResult.cause().printStackTrace();
        throw new IllegalStateException("Something went wrong during the websocket connection, handler could not be initialized.");
      }
    };
  }

  private Handler<Buffer> onEvent() {
    return buffer -> {
      // Used to log raw price
      vertx.eventBus().publish(Topics.TOPIC_RAW_MESSAGE, buffer.toString());
      // Used as internal quote
      QuoteNormalizer
        .stringToQuotos(buffer.toString())
        .stream()
        .forEach(quote -> vertx.eventBus()
          .publish(Topics.TOPIC_INTERNAL_QUOTE, quote));
    };
  }

  private Function<WebSocket, Future<WebSocket>> subscribe(ClientConfiguration config) {
    return webSocket -> {
      final JsonObject subcription = config.getSubscription();
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

  private WebSocketConnectOptions getWebSocketConfiguration(ClientConfiguration exConfig) {
    return new WebSocketConnectOptions()
      .setHost(exConfig.getHost())
      .setPort(exConfig.getPort())
      .setURI(exConfig.getPath())
      .setSsl(true);
  }
}

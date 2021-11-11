package org.broker.marketdata.client;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ClientConfiguration;
import org.broker.marketdata.configuration.ConfigurationFiles;
import org.broker.marketdata.configuration.ConfigurationLoader;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.exchange.bitmex.ExchangeHandler;
import org.broker.marketdata.protos.normalizer.QuoteNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static org.broker.marketdata.configuration.ConfigurationLoader.getConfigurationThrowableHandler;

public class WebSocketClientVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketClientVerticle.class);

  private final ExchangeHandler exchangeHandler;
  private final ConfigurationFiles configurationFiles;

  public WebSocketClientVerticle(ExchangeHandler exchangeHandler
    , ConfigurationFiles configurationFiles) {
    this.exchangeHandler = exchangeHandler;
    this.configurationFiles = configurationFiles;
  }

  private static HttpClientOptions getHttpClientOptions(ClientConfiguration config) {
    return new HttpClientOptions()
      .setMaxWebSocketFrameSize(config.getMaxWebSocketFrameSize())
      .setMaxWebSocketMessageSize(config.getMaxWebSocketMessageSize());
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    ConfigurationLoader.loadExchangeConfiguration(vertx, configurationFiles)
      .onFailure(getConfigurationThrowableHandler(startPromise))
      .onSuccess(config -> logger.info("Retrieved Configuration {}", config))
      .onComplete(startWebsocketClient(startPromise));
  }

  private Handler<AsyncResult<ClientConfiguration>> startWebsocketClient(Promise<Void> startPromise) {
    return configLoaded -> {
      if (configLoaded.succeeded()) {
        ClientConfiguration config = configLoaded.result();
        createWebSocketClient(startPromise, config, getHttpClient(config));
      } else if (configLoaded.failed()) {
        logger.error("Failure loading client configuration...");
        configLoaded.cause().printStackTrace();
        throw new IllegalStateException("Something went wrong loading configuration.");
      }
    };
  }

  private HttpClient getHttpClient(ClientConfiguration config) {
    return vertx.createHttpClient(getHttpClientOptions(config));
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
      .onComplete(eventHandler(startPromise));
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

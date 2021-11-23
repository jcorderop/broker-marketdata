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
import org.broker.marketdata.protos.normilizer.QuoteNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public abstract class AbstractWebsocketClientVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(AbstractWebsocketClientVerticle.class);

  private final ExchangeConfig exchangeConfig;
  private final QuoteNormalizer quoteNormalizer;
  private final String adapterName;

  public AbstractWebsocketClientVerticle(final ExchangeConfig exchangeConfig,
                                         final QuoteNormalizer quoteNormalizer,
                                         final String adapterName) {
    this.exchangeConfig = exchangeConfig;
    this.quoteNormalizer = quoteNormalizer;
    this.adapterName = adapterName;
    logger.info("{} configuration fetched: {}", adapterName, exchangeConfig);
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    createWebSocketClient(startPromise);
  }

  private void createWebSocketClient(final Promise<Void> startPromise) {
    logger.info("{} creating WebSocket Client...", adapterName);

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
      .setMaxWebSocketFrameSize(exchangeConfig.getMaxWebSocketFrameSize())
      .setMaxWebSocketMessageSize(exchangeConfig.getMaxWebSocketMessageSize());
  }

  private Handler<AsyncResult<WebSocket>> eventHandler(final Promise<Void> startPromise) {
    return webSocketAsyncResult -> {
      if (webSocketAsyncResult.succeeded()) {
        webSocketAsyncResult.result().handler(onEvent());
        // Till here the vertx is complete
        completeVerticle(startPromise, this.getClass().getName(), logger);
      } else if (webSocketAsyncResult.failed()) {
        logger.error("{} failure during connection...", adapterName);
        webSocketAsyncResult.cause().printStackTrace();
        throw new IllegalStateException(adapterName + " Something went wrong during the websocket connection, websocket client could not be initialized.");
      }
    };
  }

  private Handler<Buffer> onEvent() {
    return buffer -> {
      // Used as internal quote
      quoteNormalizer.stringToQuote(buffer.toString())
        .forEach(quote -> vertx.eventBus().publish(Topics.TOPIC_INTERNAL_QUOTE, quote));

      // Used to log raw price
      vertx.eventBus().publish(Topics.TOPIC_RAW_MESSAGE, buffer.toString());
    };
  }

  private Function<WebSocket, Future<WebSocket>> subscribe() {
    return webSocket -> {
      final JsonObject subcription = exchangeConfig.buildSubscription();
      logger.info("{} subscribing to: {}", adapterName, subcription.encode());
      webSocket.writeTextMessage(subcription.toString());
      return Future.succeededFuture(webSocket);
    };
  }

  private Future<WebSocket> isConnect(final WebSocket webSocket) {
    logger.info("{} is connecting...", adapterName);
    if (!webSocket.isClosed()) {
      logger.info("{} Connected!", adapterName);
    } else {
      logger.error("{} Connection Failed!", adapterName);
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

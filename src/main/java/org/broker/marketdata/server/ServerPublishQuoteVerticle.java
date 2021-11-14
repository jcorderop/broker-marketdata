package org.broker.marketdata.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.protos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerPublishQuoteVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(ServerPublishQuoteVerticle.class);

  private final QuoteBroadcast quoteBroadcast;

  public ServerPublishQuoteVerticle(QuoteBroadcast quoteBroadcast) {
    this.quoteBroadcast = quoteBroadcast;
    logger.info("ServerPublishQuoteVerticle Server Has been constructed.");
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    logger.info("Creating consumer Vertx Bus, topic: {}", Topics.TOPIC_INTERNAL_QUOTE);
    vertx.eventBus().<Quote>consumer(Topics.TOPIC_INTERNAL_QUOTE, quote -> pushQuote(quote.body()));
    completeVerticle(startPromise, this.getClass().getName(), logger);
  }

  private void pushQuote(Quote quote) {
    quoteBroadcast.getConnections().values().parallelStream().forEach(ws -> {
      logger.debug("PUBLISHING QUOTES: {}", quote);
      //ws.writeBinaryMessage(Buffer.buffer(quote.toByteArray()));
      createJsonClient(quote)
        .onFailure(event -> logger.error("Message could not be publish, {}",event.getMessage()))
        .onSuccess(ws::writeTextMessage);
    });
  }

  private Future<String> createJsonClient(final Quote quote) {
    try {
      final JsonObject jsonObject = new JsonObject(JsonFormat.printer().print(quote));
      final long publishTimestamp = System.currentTimeMillis();
      jsonObject.put("publishTimestamp", publishTimestamp);
      jsonObject.put("latency_ms", (publishTimestamp - Long.parseLong(jsonObject.getString("arrivalTimestamp"))));
      return Future.succeededFuture(jsonObject.encode());
    } catch (InvalidProtocolBufferException e) {
      return Future.failedFuture(e);
    }
  }
}

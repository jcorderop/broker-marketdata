package org.broker.marketdata.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.protos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerPublishQuoteVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(ServerPublishQuoteVerticle.class);

  private final QuoteBroadcast quoteBroadcast;

  public ServerPublishQuoteVerticle(final QuoteBroadcast quoteBroadcast) {
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

  private void pushQuote(final Quote quote) {
    final var finalQuote = Quote.newBuilder(quote)
      .setPublishTimestamp(System.currentTimeMillis())
      .build();
    //ws.writeBinaryMessage(Buffer.buffer(quote.toByteArray()));
    brodcastMessage(finalQuote)
      .onComplete(event -> vertx.eventBus()
        .publish(Topics.TOPIC_STORAGE, finalQuote));
  }

  private Future<Void> brodcastMessage(final Quote finalQuote) {
    quoteBroadcast.getConnections()
      .values()
      .parallelStream()
      .forEach(ws -> {
        logger.debug("PUBLISHING QUOTES: {}", finalQuote);
        try {
          ws.writeTextMessage(JsonFormat.printer().print(finalQuote));
        } catch (InvalidProtocolBufferException e) {
          logger.warn("Quote could not be publish...");
        }
      });
    return Future.succeededFuture();
  }
}

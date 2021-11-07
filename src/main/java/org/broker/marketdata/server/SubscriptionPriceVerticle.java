package org.broker.marketdata.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.protos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionPriceVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(SubscriptionPriceVerticle.class);

  private final PriceBroadcast broadcast;

  public SubscriptionPriceVerticle(PriceBroadcast broadcast) {
    this.broadcast = broadcast;
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    vertx.eventBus().<Quote>consumer(Topics.TOPIC_INTERNAL_QUOTE, quote -> pushQuote(quote.body()));
    completeVerticle(startPromise, this.getClass().getName(), logger);
  }

  private void pushQuote(Quote quote) {
    for (ServerWebSocket ws : broadcast.getConnections().values()) {
      logger.debug("PUBLISHING QUOTES: {}",quote);
      try {
        //ws.writeBinaryMessage(Buffer.buffer(quote.toByteArray()));
        ws.writeTextMessage(JsonFormat.printer().print(quote));
      } catch (InvalidProtocolBufferException e) {
        logger.error("Quote could not be converted to json, ", e.getMessage());
      }
    }
  }
}

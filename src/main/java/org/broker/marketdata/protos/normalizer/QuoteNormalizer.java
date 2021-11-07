package org.broker.marketdata.protos.normalizer;

import com.google.protobuf.Descriptors;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.broker.marketdata.exchange.bitmex.BitmexHandler;
import org.broker.marketdata.protos.InternalQuote;
import org.broker.marketdata.protos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QuoteNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(QuoteNormalizer.class);

  public static List<Quote> stringToQuotos(String message) {
    return toQuoteProto(message);
  }

  private static List<Quote> toQuoteProto(final String message) {
    final DocumentContext messageContext = JsonPath.parse(message);

    final var topic = getStringValue(messageContext, BitmexHandler.JSON_TABLE.getString(1));
    final var action = getStringValue(messageContext, BitmexHandler.JSON_ACTION.getString(1));

    if (topic.isEmpty()) {
      return new ArrayList<>();
    }

    return ((List < Object >) messageContext.read("$.data"))
      .stream()
      .map(tick -> {
        final DocumentContext quoteContext = JsonPath.parse(tick);
        final var quote = Quote.newBuilder();

        quote.setArrivalTimestamp(System.currentTimeMillis());
        quote.setSource(BitmexHandler.SOURCE);
        topic.ifPresent(s -> quote.setTopic(s));
        action.ifPresent(s -> quote.setAction(s));

        getStringValue(quoteContext, BitmexHandler.JSON_SYMBOL.getString(1)).ifPresent(s -> quote.setSymbol(s));
        getDoubleValue(quoteContext, BitmexHandler.JSON_MARK_PRICE.getString(1)).ifPresent(s -> quote.setMarkPrice(s));
        getDoubleValue(quoteContext, BitmexHandler.JSON_BID_PRICE.getString(1)).ifPresent(s -> quote.setBidPrice(s));
        getDoubleValue(quoteContext, BitmexHandler.JSON_MID_PRICE.getString(1)).ifPresent(s -> quote.setMidPrice(s));
        getDoubleValue(quoteContext, BitmexHandler.JSON_ASK_PRICE.getString(1)).ifPresent(s -> quote.setAskPrice(s));
        getDoubleValue(quoteContext, BitmexHandler.JSON_VOLUME.getString(1)).ifPresent(s -> quote.setVolume(s));

        return quote.build();
      }).collect(Collectors.toList());
  }

  private static Optional<String> getStringValue(final DocumentContext ctx, final String fieldName) {
    try {
      return Optional.ofNullable(ctx.read(fieldName, String.class));
    } catch (Exception e) {
      logger.debug("Field {} could not be parse from source.", fieldName);
    }
    return Optional.empty();
  }

  private static Optional<Double> getDoubleValue(final DocumentContext ctx, final String fieldName) {
    try {
      return Optional.ofNullable(ctx.read(fieldName, Double.class));
    } catch (Exception e) {
      logger.debug("Field {} could not be parse from source.", fieldName);
    }
    return Optional.empty();
  }
}

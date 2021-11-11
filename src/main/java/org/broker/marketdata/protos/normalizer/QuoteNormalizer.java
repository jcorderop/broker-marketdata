package org.broker.marketdata.protos.normalizer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.broker.marketdata.exchange.bitmex.BitmexHandler;
import org.broker.marketdata.protos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QuoteNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(QuoteNormalizer.class);
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
  private static final JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();

  public static List<Quote> stringToQuote(String message) {
    try {
      return parse(message);
    } catch (Exception e) {
      logger.warn("Could not parse the source quote, {}", e.getMessage());
      return new ArrayList<>();
    }
  }

  private static List<Quote> parse(String message) {
    final DocumentContext messageContext = JsonPath.parse(message);

    final var topic = getStringValue(messageContext, BitmexHandler.JSON_TABLE.getString(1));
    final var action = getStringValue(messageContext, BitmexHandler.JSON_ACTION.getString(1));

    return ((List<Object>) messageContext.read("$.data"))
      .stream()
      .map(createInternalQuote(topic, action)).collect(Collectors.toList());
  }

  private static Function<Object, Quote> createInternalQuote(Optional<String> topic,
                                                             Optional<String> action) {
    return tick -> {
      final DocumentContext messageContext = JsonPath.parse(tick);

      final var quote = Quote.newBuilder();

      setCustomFields(topic, action, quote, messageContext);
      try {
        mergeJsonToProto(JsonPath.parse(tick).jsonString(), quote);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }

      internalQuoteValidation(quote);

      return quote.build();
    };
  }

  private static void setCustomFields(Optional<String> topic,
                                      Optional<String> action,
                                      Quote.Builder quote,
                                      DocumentContext messageContext) {

    quote.setArrivalTimestamp(System.currentTimeMillis());
    quote.setSource(BitmexHandler.SOURCE);
    quote.setTopic(topic.orElseThrow(() -> new IllegalStateException("Topic not present.")));
    quote.setAction(action.orElseThrow(() -> new IllegalStateException("Action not present.")));

    //https://stackoverflow.com/questions/1712205/current-time-in-microseconds-in-java#:~:text=No%2C%20Java%20doesn%27t%20have%20that%20ability.%20It%20does,use%20it%20to%20measure%20nanosecond%20%28or%20higher%29%20precision.
    quote.setSourceTimestamp(getLocalDateTimeLong(messageContext, BitmexHandler.JSON_TIMESTAMP.getString(1))
      .orElseThrow(() -> new IllegalStateException("Timestamp not present.")));
  }

  private static void internalQuoteValidation(Quote.Builder quote) {
    if (!quote.hasSymbol()) throw new IllegalStateException("Symbol not present.");
  }

  private static void mergeJsonToProto(final String json
    , final Quote.Builder builder) throws InvalidProtocolBufferException {
    //https://itnext.io/protobuf-and-null-support-1908a15311b6
    parser.merge(json, builder);
    logger.debug("Json merged into proto quote {}", printer.print(builder));
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

  private static Optional<Long> getLocalDateTimeLong(final DocumentContext ctx, final String fieldName) {
    try {
      return Optional.of(new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toFormatter()
        .parse(getStringValue(ctx, fieldName).get(), Instant::from)
        .toEpochMilli());
    } catch (Exception e) {
      logger.debug("Field {} could not be parse from source.", fieldName);
    }
    return Optional.empty();
  }
}

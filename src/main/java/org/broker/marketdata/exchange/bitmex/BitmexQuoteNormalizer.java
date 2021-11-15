package org.broker.marketdata.exchange.bitmex;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.RequiredArgsConstructor;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.protos.normilizer.QuoteNormalizer;
import org.broker.marketdata.protos.normilizer.QuoteNormalizerCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class BitmexQuoteNormalizer implements QuoteNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(BitmexQuoteNormalizer.class);

  private final BitmexConfig bitmexConfig;

  public List<Quote> stringToQuote(String message) {
    try {
      return parse(message);
    } catch (Exception e) {
      logger.warn("Could not parse the source quote, {}", e.getMessage());
      return new ArrayList<>();
    }
  }

  private List<Quote> parse(String message) {
    final DocumentContext messageContext = JsonPath.parse(message);

    final var topic = QuoteNormalizerCommon.getStringValue(messageContext, BitmexConfig.JSON_TABLE.getString(1));
    final var action = QuoteNormalizerCommon.getStringValue(messageContext, BitmexConfig.JSON_ACTION.getString(1));

    return ((List<Object>) messageContext.read("$.data"))
      .stream()
      .map(createInternalQuote(topic, action)).collect(Collectors.toList());
  }

  private Function<Object, Quote> createInternalQuote(Optional<String> topic,
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

      QuoteNormalizerCommon.internalQuoteValidation(quote);

      return quote.build();
    };
  }

  private void setCustomFields(Optional<String> topic,
                               Optional<String> action,
                               Quote.Builder quote,
                               DocumentContext messageContext) {
    quote.setArrivalTimestamp(System.currentTimeMillis());
    quote.setQuoteId(System.nanoTime());
    quote.setStage(Quote.StageType.RAW);
    quote.setSource(bitmexConfig.getSource());
    quote.setTopic(topic.orElseThrow(() -> new IllegalStateException("Topic not present.")));
    quote.setAction(action.orElseThrow(() -> new IllegalStateException("Action not present.")));

    //https://stackoverflow.com/questions/1712205/current-time-in-microseconds-in-java#:~:text=No%2C%20Java%20doesn%27t%20have%20that%20ability.%20It%20does,use%20it%20to%20measure%20nanosecond%20%28or%20higher%29%20precision.
    quote.setSourceTimestamp(getLocalDateTimeLong(messageContext, BitmexConfig.JSON_TIMESTAMP.getString(1))
      .orElseThrow(() -> new IllegalStateException("Timestamp not present.")));
  }

  private static void mergeJsonToProto(final String json
    , final Quote.Builder builder) throws InvalidProtocolBufferException {
    //https://itnext.io/protobuf-and-null-support-1908a15311b6
    QuoteNormalizerCommon.parser.merge(json, builder);
    logger.debug("Json merged into proto quote {}", QuoteNormalizerCommon.printer.print(builder));
  }
  private static Optional<Long> getLocalDateTimeLong(final DocumentContext ctx, final String fieldName) {
    try {
      return Optional.of(new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toFormatter()
        .parse(QuoteNormalizerCommon.getStringValue(ctx, fieldName).get(), Instant::from)
        .toEpochMilli());
    } catch (Exception e) {
      logger.debug("Field {} could not be parse from source.", fieldName);
    }
    return Optional.empty();
  }
}

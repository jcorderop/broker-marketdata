package org.broker.marketdata.protos.normilizer;

import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.DocumentContext;
import org.broker.marketdata.protos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class QuoteNormalizerCommon {

  private static Logger logger = LoggerFactory.getLogger(QuoteNormalizerCommon.class);

  public static JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
  public static JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();

  public static void internalQuoteValidation(final Quote.Builder quote) {
    if (!quote.hasSymbol()) throw new IllegalStateException("Symbol not present.");
  }

  public static Optional<String> getStringValue(final DocumentContext ctx,
                                                final String fieldName) {
    try {
      return Optional.ofNullable(ctx.read(fieldName, String.class));
    } catch (Exception e) {
      logger.debug("Field {} could not be parse from source.", fieldName);
    }
    return Optional.empty();
  }

  public static Optional<Double> getDoubleValue(final DocumentContext ctx,
                                                final String fieldName) {
    try {
      return Optional.ofNullable(ctx.read(fieldName, Double.class));
    } catch (Exception e) {
      logger.debug("Field {} could not be parse from source.", fieldName);
    }
    return Optional.empty();
  }
}

package org.broker.marketdata.exchange.binance;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.RequiredArgsConstructor;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.protos.normilizer.QuoteNormalizer;
import org.broker.marketdata.protos.normilizer.QuoteNormalizerCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class BinanceQuoteNormalizer implements QuoteNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(BinanceQuoteNormalizer.class);

  private final BinanceConfig binanceConfig;

  public List<Quote> stringToQuote(final String message) {
    try {
      return parse(message);
    } catch (Exception e) {
      logger.warn("Could not parse the source quote, {}", e.getCause().getMessage());
      return new ArrayList<>();
    }
  }

  private List<Quote> parse(final String message) {
    final var quote = Quote.newBuilder();
    setCustomFields(quote);
    setJsonMessages(message, quote);
    setAlculatedValues(quote);
    QuoteNormalizerCommon.internalQuoteValidation(quote);
    return List.of(quote.build());
  }

  private void setJsonMessages(final String message, final Quote.Builder quote) {
    final DocumentContext messageContext = JsonPath.parse(message);
    quote.setAction(BinanceConfig.JSON_ACTION_UPDATE);
    quote.setTopic(BinanceConfig.JSON_TOPIC_INSTRUMENT);
    quote.setBidPrice(Double.parseDouble(messageContext.read(BinanceConfig.JSON_BID_PRICE)));
    quote.setAskPrice(Double.parseDouble(messageContext.read(BinanceConfig.JSON_ASK_PRICE)));
    quote.setSymbol(binanceConfig.getSymbol().get(messageContext.read(BinanceConfig.JSON_SYMBOL)));
  }

  private void setAlculatedValues(final Quote.Builder quote) {
    quote.setMidPrice((quote.getAskPrice() + quote.getBidPrice()) / 2);
  }

  private void setCustomFields(final Quote.Builder quote) {
    quote.setSourceTimestamp(System.currentTimeMillis());
    quote.setArrivalTimestamp(System.currentTimeMillis());
    quote.setQuoteId(System.nanoTime());
    quote.setStage(Quote.StageType.RAW);
    quote.setSource(binanceConfig.getSource());
  }
}

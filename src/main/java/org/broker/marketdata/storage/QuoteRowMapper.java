package org.broker.marketdata.storage;

import org.broker.marketdata.protos.Quote;

public class QuoteRowMapper {
  public static QuoteEntity mapQuoteEntityFromQuoteProto (Quote quote) {
    return new QuoteEntity.QuoteEntityBuilder()
      .quoteId(quote.getQuoteId())
      .source(quote.getSource())
      .topic(quote.getTopic())
      .action(quote.getAction())
      .stage(quote.getStage().name())
      .symbol(quote.getSymbol())

      .markPrice(quote.hasMarkPrice() ? quote.getMarkPrice() : null)
      .bidPrice(quote.hasBidPrice() ? quote.getBidPrice() : null)
      .midPrice(quote.hasMidPrice() ? quote.getMidPrice() : null)
      .askPrice(quote.hasAskPrice() ? quote.getAskPrice() : null)
      .volume(quote.hasVolume() ? quote.getVolume() : null)

      .sourceTimestamp(quote.getSourceTimestamp())
      .arrivalTimestamp(quote.getArrivalTimestamp())
      .publishTimestamp(quote.getPublishTimestamp())
      .build();
  }
}

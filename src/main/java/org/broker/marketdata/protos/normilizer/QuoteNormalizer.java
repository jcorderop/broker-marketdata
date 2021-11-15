package org.broker.marketdata.protos.normilizer;

import org.broker.marketdata.protos.Quote;

import java.util.List;

public interface QuoteNormalizer {
  List<Quote> stringToQuote(String message);
}

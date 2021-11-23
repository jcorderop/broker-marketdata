package org.broker.marketdata.exchange.bitmex;

import org.broker.marketdata.client.AbstractWebsocketClientVerticle;
import org.broker.marketdata.configuration.ExchangeConfig;
import org.broker.marketdata.protos.normilizer.QuoteNormalizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BitmexAdapter extends AbstractWebsocketClientVerticle {

  @Autowired
  private final ExchangeConfig bitmexConfig;
  @Autowired
  private final QuoteNormalizer bitmexQuoteNormalizer;

  public BitmexAdapter(final ExchangeConfig bitmexConfig, final QuoteNormalizer bitmexQuoteNormalizer) {
    super(bitmexConfig, bitmexQuoteNormalizer, BitmexAdapter.class.getSimpleName());
    this.bitmexConfig = bitmexConfig;
    this.bitmexQuoteNormalizer = bitmexQuoteNormalizer;
  }
}

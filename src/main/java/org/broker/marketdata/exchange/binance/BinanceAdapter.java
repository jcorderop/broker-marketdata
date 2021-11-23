package org.broker.marketdata.exchange.binance;

import org.broker.marketdata.client.AbstractWebsocketClientVerticle;
import org.broker.marketdata.configuration.ExchangeConfig;
import org.broker.marketdata.protos.normilizer.QuoteNormalizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BinanceAdapter extends AbstractWebsocketClientVerticle {

  @Autowired
  private final ExchangeConfig binanceConfig;
  @Autowired
  private final QuoteNormalizer binanceQuoteNormalizer;

  public BinanceAdapter(final ExchangeConfig binanceConfig, final QuoteNormalizer binanceQuoteNormalizer) {
    super(binanceConfig, binanceQuoteNormalizer, BinanceAdapter.class.getSimpleName());
    this.binanceConfig = binanceConfig;
    this.binanceQuoteNormalizer = binanceQuoteNormalizer;
  }
}

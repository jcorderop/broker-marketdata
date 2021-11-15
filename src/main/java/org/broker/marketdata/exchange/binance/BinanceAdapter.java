package org.broker.marketdata.exchange.binance;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.broker.marketdata.client.WebsocketClientVerticle;
import org.broker.marketdata.common.VerticleCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BinanceAdapter extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(BinanceAdapter.class);

  @Autowired
  private BinanceConfig binanceConfig;
  @Autowired
  private BinanceQuoteNormalizer binanceQuoteNormalizer;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    createWebSocketClient(startPromise);
  }

  private void createWebSocketClient(Promise<Void> startPromise) {
    vertx.deployVerticle(new WebsocketClientVerticle(binanceConfig, binanceQuoteNormalizer))
      .onFailure(event -> {
        logger.error("Binance Adapter failure, component could not be deployed...");
        event.printStackTrace();
        startPromise.fail(event);
      })
      .onSuccess(event -> {
        logger.info("Binance Adapter has been deployed successfully...");
        completeVerticle(startPromise, this.getClass().getName(), logger);
      });
  }
}

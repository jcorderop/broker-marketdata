package org.broker.marketdata.exchange.bitmex;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.broker.marketdata.client.WebsocketClientVerticle;
import org.broker.marketdata.common.VerticleCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BitmexAdapter extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(BitmexAdapter.class);

  @Autowired
  private BitmexConfig bitmexConfig;
  @Autowired
  private BitmexQuoteNormalizer bitmexQuoteNormalizer;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    createWebSocketClient(startPromise);
  }

  private void createWebSocketClient(Promise<Void> startPromise) {
    vertx.deployVerticle(new WebsocketClientVerticle(bitmexConfig, bitmexQuoteNormalizer))
       .onFailure(event -> {
         logger.error("Bitmex Adapter failure, component could not be deployed...");
         event.printStackTrace();
         startPromise.fail(event);
       })
       .onSuccess(event -> {
         logger.info("Bitmex Adapter has been deployed successfully...");
         completeVerticle(startPromise, this.getClass().getName(), logger);
       });
  }
}

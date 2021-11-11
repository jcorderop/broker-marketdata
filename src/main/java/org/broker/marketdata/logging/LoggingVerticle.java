package org.broker.marketdata.logging;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ConfigurationFiles;
import org.broker.marketdata.configuration.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(LoggingVerticle.class.getSimpleName());

  private final ConfigurationFiles configurationFiles;

  public LoggingVerticle(ConfigurationFiles configurationFiles) {
    this.configurationFiles = configurationFiles;
  }


  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    vertx.eventBus().<String>consumer(Topics.TOPIC_RAW_MESSAGE, message -> logger.info(message.body()));
    completeVerticle(startPromise, this.getClass().getName(), logger);
  }
}

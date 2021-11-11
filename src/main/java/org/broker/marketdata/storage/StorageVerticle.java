package org.broker.marketdata.storage;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.broker.marketdata.common.VerticleCommon;
import org.broker.marketdata.configuration.ConfigurationFiles;
import org.broker.marketdata.configuration.ConfigurationLoader;
import org.broker.marketdata.configuration.Topics;
import org.broker.marketdata.protos.Quote;
import org.broker.marketdata.storage.db.DBPools;
import org.broker.marketdata.storage.db.migration.FlywayMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.broker.marketdata.configuration.ConfigurationLoader.getConfigurationThrowableHandler;

public class StorageVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(StorageVerticle.class);

  private Pool pgPool;

  private final ConfigurationFiles configurationFiles;

  public StorageVerticle(ConfigurationFiles configurationFiles) {
    this.configurationFiles = configurationFiles;
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    ConfigurationLoader.loadDatabaseConfiguration(vertx, configurationFiles)
      .onFailure(getConfigurationThrowableHandler(startPromise))
      .compose(config -> FlywayMigration.migrate(vertx, config)
        .onFailure(startPromise::fail)
        .onSuccess(unused -> {
          logger.info("Retrieved Configuration {}", config);
          pgPool = DBPools.createPgPool(vertx, config);
          vertx.eventBus().consumer(Topics.TOPIC_INTERNAL_QUOTE, this::store);
          completeVerticle(startPromise, this.getClass().getName(), logger);
        }))
      .onFailure(throwable -> {
        logger.error("Non handle exception loading database, {}", throwable.getMessage());
        throwable.printStackTrace();
      });
  }

  private void store(Message<Quote> quote) {
    pgPool.withTransaction(sqlConnection -> insertQuote(sqlConnection, quote));
  }

  private Future<SqlResult<Void>> insertQuote(SqlConnection sqlConnection, Message<Quote> body) {

    final List<Map<String, Object>> parameterBatch = getParameterBatch(body.body());

    return SqlTemplate.forUpdate(sqlConnection,
        "INSERT INTO broker.quotes (source, topic, symbol, action, mark_price, bid_price, mid_price, ask_price, volume)" +
          " VALUES (#{source}, #{topic}, #{symbol}, #{action}, #{mark_price}, #{bid_price}, #{mid_price}, #{ask_price}, #{volume})")
      .executeBatch(parameterBatch)
      .onFailure(throwable -> {
        logger.error("DB error, {}", throwable.getMessage());
        throwable.printStackTrace();
      })
      .onSuccess(result -> logger.debug("Stored {}", result.rowCount()));
  }

  private List<Map<String, Object>> getParameterBatch(final Quote quote) {
    logger.debug(quote.toString());
    final HashMap<String, Object> parameters = new HashMap<>();
    parameters.put("source", quote.getSource());
    parameters.put("topic", quote.getTopic());
    parameters.put("action", quote.getAction());
    parameters.put("symbol", quote.getSymbol());
    parameters.put("mark_price", quote.hasMarkPrice() ? quote.getMarkPrice() : null);
    parameters.put("bid_price", quote.hasBidPrice() ? quote.getBidPrice(): null);
    parameters.put("mid_price", quote.hasMidPrice() ? quote.getMidPrice() : null);
    parameters.put("ask_price", quote.hasAskPrice() ? quote.getAskPrice() : null);
    parameters.put("volume", quote.hasVolume() ? quote.getVolume() : null);
    logger.debug("parameters: {}", parameters);
    return List.of(parameters);
  }
}

package org.broker.marketdata.storage;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.broker.marketdata.bus.LocalMessageCodec;
import org.broker.marketdata.client.WebSocketClientVerticle;
import org.broker.marketdata.common.VerticleCommon;
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
import java.util.stream.Collectors;

import static org.broker.marketdata.configuration.ConfigurationLoader.*;

public class StorageVerticle extends AbstractVerticle implements VerticleCommon {

  private static final Logger logger = LoggerFactory.getLogger(StorageVerticle.class);

  private Pool pgPool;

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    deforeStartVerticle(logger, this.getClass().getName());
    ConfigurationLoader.loadDatabaseConfiguration(vertx)
      .onFailure(throwable -> {
        logger.error("Could not load Database configuration, ", throwable.getMessage());
        throwable.printStackTrace();
      })
      .compose(config -> FlywayMigration.migrate(vertx, config)
          .onFailure(startPromise::fail)
          .onSuccess(unused -> {
            pgPool = DBPools.createPgPool(vertx, config);
            vertx.eventBus()
              .<Quote>consumer(Topics.TOPIC_INTERNAL_QUOTE, quote -> store(quote));
            completeVerticle(startPromise, this.getClass().getName(), logger);
          }))
      .onFailure(throwable -> {
        logger.error("Non handle exception loading database, ", throwable.getMessage());
        throwable.printStackTrace();
      });
  }

  private void store(Message<Quote> quote) {
    pgPool.withTransaction(sqlConnection -> {
      return insertQuote(sqlConnection, quote);
    });
  }


  private Future<SqlResult<Void>> insertQuote(SqlConnection sqlConnection, Message<Quote> body) {

    final List<Map<String, Object>> parameterBatch = getParameterBatch(body.body());

    Future<SqlResult<Void>> sqlResultFuture = SqlTemplate.forUpdate(sqlConnection,
        "INSERT INTO broker.quotes (source, topic, symbol, action, mark_price, bid_price, mid_price, ask_price, volume)" +
          " VALUES (#{source}, #{topic}, #{symbol}, #{action}, #{mark_price}, #{bid_price}, #{mid_price}, #{ask_price}, #{volume})")
      .executeBatch(parameterBatch)
      .onFailure(throwable -> {
        logger.error("DB error, ",throwable.getMessage());
        throwable.printStackTrace();
      })
      .onSuccess(result -> logger.debug("Stored {}", result.rowCount()));

    return sqlResultFuture;
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

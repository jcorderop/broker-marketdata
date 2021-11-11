package org.broker.marketdata.storage.db;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.broker.marketdata.configuration.DatabaseConfiguration;

public class DBPools {

  private DBPools() {}

  public static Pool createPgPool(final Vertx vertx, final DatabaseConfiguration config) {
    final var connectOptions = new PgConnectOptions()
      .setHost(config.getHost())
      .setPort(config.getPort())
      .setDatabase(config.getDatabase())
      .setUser(config.getUser())
      .setPassword(config.getPassword());

    var poolOptions = new PoolOptions()
      .setMaxSize(4);

    return PgPool.pool(vertx, connectOptions, poolOptions);
  }
}

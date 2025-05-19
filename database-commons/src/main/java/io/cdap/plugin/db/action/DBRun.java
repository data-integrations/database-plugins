/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.db.action;

import dev.failsafe.RetryPolicy;
import io.cdap.plugin.common.db.DBErrorDetailsProvider;
import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.DriverCleanup;
import io.cdap.plugin.util.RetryPolicyUtil;
import io.cdap.plugin.util.RetryUtils;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Class used by database action plugins to run database commands
 */
public class DBRun {
  private final QueryConfig config;
  private final Class<? extends Driver> driverClass;
  private boolean enableAutoCommit;
  private final RetryPolicy<?> retryPolicy;
  protected DBErrorDetailsProvider dbErrorDetailsProvider;

  public DBRun(QueryConfig config, Class<? extends Driver> driverClass, Boolean enableAutocommit) {
    this.config = config;
    this.driverClass = driverClass;
    if (enableAutocommit != null) {
      this.enableAutoCommit = enableAutocommit;
    }
    this.retryPolicy = RetryPolicyUtil.getRetryPolicy(config.getInitialRetryDuration(), config.getMaxRetryDuration(),
      config.getMaxRetryCount());
  }

  /**
   * Returns the DBErrorDetailsProvider instance.
   *
   * @return DBErrorDetailsProvider instance
   */
  protected DBErrorDetailsProvider getErrorDetailsProvider() {
    if (dbErrorDetailsProvider == null) {
      dbErrorDetailsProvider =  new DBErrorDetailsProvider();
    }
    return dbErrorDetailsProvider;
  }

  /**
   * Uses a configured JDBC driver to execute a SQL statement. The configurations of which JDBC driver
   * to use and which connection string to use come from the plugin configuration.
   */
  public void run() throws SQLException, InstantiationException, IllegalAccessException {
    DriverCleanup driverCleanup = null;
    try {
      driverCleanup = DBUtils.ensureJDBCDriverIsAvailable(driverClass, config.getConnectionString(),
                                                          config.getJdbcPluginName());

      Properties connectionProperties = new Properties();
      connectionProperties.putAll(config.getConnectionArguments());
      try (Connection connection = RetryUtils.createConnectionWithRetry((RetryPolicy<Connection>) retryPolicy,
        config.getConnectionString(), connectionProperties, getErrorDetailsProvider())) {
        executeInitQueries(connection, config.getInitQueries());
        if (!enableAutoCommit) {
          connection.setAutoCommit(false);
        }
        try (Statement statement = RetryUtils.createStatementWithRetry((RetryPolicy<Statement>) retryPolicy, connection,
          getErrorDetailsProvider())) {
          RetryUtils.executeInitQueryWithRetry(retryPolicy, statement, config.query, getErrorDetailsProvider());
          if (!enableAutoCommit) {
            connection.commit();
          }
        }
      }
    } finally {
      if (driverCleanup != null) {
        driverCleanup.destroy();
      }
    }
  }

  private void executeInitQueries(Connection connection, List<String> initQueries) throws SQLException {
    for (String query : initQueries) {
      try (Statement statement = RetryUtils.createStatementWithRetry((RetryPolicy<Statement>) retryPolicy, connection,
        getErrorDetailsProvider())) {
        RetryUtils.executeInitQueryWithRetry(retryPolicy, statement, query, getErrorDetailsProvider());
      }
    }
  }
}

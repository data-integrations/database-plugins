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

package io.cdap.plugin.db.batch.action;

import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.DriverCleanup;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
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

  public DBRun(QueryConfig config, Class<? extends Driver> driverClass, Boolean enableAutocommit) {
    this.config = config;
    this.driverClass = driverClass;
    if (enableAutocommit != null) {
      this.enableAutoCommit = enableAutocommit;
    }
  }

  /**
   * Uses a configured JDBC driver to execute a SQL statement. The configurations of which JDBC driver
   * to use and which connection string to use come from the plugin configuration.
   */
  public void run() throws SQLException, InstantiationException, IllegalAccessException {
    DriverCleanup driverCleanup = null;
    try {
      driverCleanup = DBUtils.ensureJDBCDriverIsAvailable(driverClass, config.getConnectionString(),
                                                          config.jdbcPluginName);

      Properties connectionProperties = new Properties();
      connectionProperties.putAll(config.getConnectionArguments());
      try (Connection connection = DriverManager.getConnection(config.getConnectionString(), connectionProperties)) {
        executeInitQueries(connection, config.getInitQueries());
        if (!enableAutoCommit) {
          connection.setAutoCommit(false);
        }
        try (Statement statement = connection.createStatement()) {
          statement.execute(config.query);
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
      try (Statement statement = connection.createStatement()) {
        statement.execute(query);
      }
    }
  }
}

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

package io.cdap.plugin.db.source;

import com.google.common.base.Throwables;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.JDBCDriverShim;
import io.cdap.plugin.db.NoOpCommitConnection;
import io.cdap.plugin.db.RollbackOnCloseConnection;
import io.cdap.plugin.db.TransactionIsolationLevel;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Class that extends {@link DBInputFormat} to load the database driver class correctly.
 */
public class DataDrivenETLDBInputFormat extends DataDrivenDBInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(DataDrivenETLDBInputFormat.class);
  private Driver driver;
  private JDBCDriverShim driverShim;

  public static void setInput(Configuration conf,
                              Class<? extends DBWritable> inputClass,
                              String inputQuery,
                              String inputBoundingQuery,
                              boolean enableAutoCommit) {
    DBConfiguration dbConf = new DBConfiguration(conf);
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputBoundingQuery(inputBoundingQuery);
    new ConnectionConfigAccessor(conf).setAutoCommitEnabled(enableAutoCommit);
  }

  @Override
  public Connection getConnection() {
    if (this.connection == null) {
      ConnectionConfigAccessor connectionConfigAccessor = new ConnectionConfigAccessor(getConf());
      Connection conn = null;
      try {
        String url = connectionConfigAccessor.getConfiguration().get(DBConfiguration.URL_PROPERTY);
        try {
          // throws SQLException if no suitable driver is found
          DriverManager.getDriver(url);
        } catch (SQLException e) {
          if (driverShim == null) {
            if (driver == null) {
              ClassLoader classLoader = connectionConfigAccessor.getConfiguration().getClassLoader();
              String driverClassName = connectionConfigAccessor.getConfiguration()
                .get(DBConfiguration.DRIVER_CLASS_PROPERTY);
              @SuppressWarnings("unchecked")
              Class<? extends Driver> driverClass =
                (Class<? extends Driver>) classLoader.loadClass(driverClassName);
              driver = driverClass.newInstance();

              // De-register the default driver that gets registered when driver class is loaded.
              DBUtils.deregisterAllDrivers(driverClass);
            }
            driverShim = new JDBCDriverShim(driver);
            DriverManager.registerDriver(driverShim);
            LOG.debug("Registered JDBC driver via shim {}. Actual Driver {}.", driverShim, driver);
          }
        }

        Properties properties = new Properties();
        properties.putAll(connectionConfigAccessor.getConnectionArguments());
        conn = DriverManager.getConnection(url, properties);
        conn = configureTransactionMode(conn, connectionConfigAccessor);

        if (connectionConfigAccessor.getFetchSize() > 0) {
          conn = new ConnectionWithFetchSize(conn, connectionConfigAccessor.getFetchSize());
        }

        String level = connectionConfigAccessor.getConfiguration().get(TransactionIsolationLevel.CONF_KEY);
        LOG.debug("Transaction isolation level: {}", level);
        conn.setTransactionIsolation(TransactionIsolationLevel.getLevel(level));
        // execute initialization queries if any
        for (String query : connectionConfigAccessor.getInitQueries()) {
          try (Statement statement = conn.createStatement()) {
            statement.execute(query);
          }
        }
        this.connection = conn;
      } catch (Exception e) {
        closeQuietly(conn);
        deregisterDriverShimQuietly();
        throw Throwables.propagate(e);
      }
    }
    return this.connection;
  }

  private static Connection configureTransactionMode(Connection conn,
                                                     ConnectionConfigAccessor accessor) throws SQLException {
    if (!accessor.isReadOnly()) {
      if (accessor.isAutoCommitEnabled()) {
        // hack to work around jdbc drivers like the hive driver that throw exceptions on commit
        return new NoOpCommitConnection(conn);
      }
      conn.setAutoCommit(false);
      return conn;
    }

    boolean disabledAutoCommit = false;
    if (!accessor.isAutoCommitEnabled()) {
      try {
        conn.setAutoCommit(false);
        disabledAutoCommit = true;
      } catch (SQLFeatureNotSupportedException e) {
        LOG.debug("Driver {} does not support disabling auto-commit; continuing.",
                  conn.getClass().getName(), e);
      } catch (SQLException e) {
        LOG.warn("Failed to disable auto-commit on {}; continuing with auto-commit.",
                 conn.getClass().getName(), e);
      }
    }
    setReadOnlyQuietly(conn);
    return disabledAutoCommit ? new RollbackOnCloseConnection(conn) : new NoOpCommitConnection(conn);
  }

  private static void setReadOnlyQuietly(Connection conn) {
    try {
      conn.setReadOnly(true);
    } catch (SQLFeatureNotSupportedException e) {
      LOG.debug("Driver {} does not support setting connection read-only; continuing.",
                conn.getClass().getName(), e);
    } catch (SQLException e) {
      LOG.warn("Failed to set connection {} read-only; continuing.",
               conn.getClass().getName(), e);
    }
  }

  private static void closeQuietly(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException closeEx) {
        LOG.warn("Failed to close JDBC connection {} after connection setup failure.",
                 conn.getClass().getName(), closeEx);
      }
    }
  }

  private void deregisterDriverShimQuietly() {
    if (driverShim != null) {
      try {
        DriverManager.deregisterDriver(driverShim);
      } catch (SQLException deregisterEx) {
        LOG.warn("Failed to deregister JDBC driver shim {} after connection setup failure.",
                 driverShim, deregisterEx);
      } finally {
        driverShim = null;
      }
    }
  }

  // versions > HDP-2.3.4 started using createConnection instead of getConnection,
  // this is added for compatibility, more information at (HYDRATOR-791)
  public Connection createConnection() {
    return getConnection();
  }

  @Override
  public RecordReader createDBRecordReader(DBInputSplit split, Configuration conf) throws IOException {
    final RecordReader dbRecordReader = super.createDBRecordReader(split, conf);
    return new RecordReader() {
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        dbRecordReader.initialize(split, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return dbRecordReader.nextKeyValue();
      }

      @Override
      public Object getCurrentKey() throws IOException, InterruptedException {
        return dbRecordReader.getCurrentKey();
      }

      @Override
      public Object getCurrentValue() throws IOException, InterruptedException {
        return dbRecordReader.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return dbRecordReader.getProgress();
      }

      @Override
      public void close() throws IOException {
        Throwable primary = null;
        try {
          dbRecordReader.close();
        } catch (Throwable t) {
          primary = t;
          throw t;
        } finally {
          try {
            DriverManager.deregisterDriver(driverShim);
            driverShim = null;
          } catch (SQLException e) {
            if (primary != null) {
              primary.addSuppressed(e);
            } else {
              throw new IOException(e);
            }
          }
        }
      }
    };
  }

  @Override
  protected void closeConnection() {
    Throwable primary = null;
    try {
      super.closeConnection();
    } catch (Throwable t) {
      primary = t;
      throw t;
    } finally {
      try {
        DriverManager.deregisterDriver(driverShim);
        driverShim = null;
      } catch (SQLException e) {
        if (primary != null) {
          primary.addSuppressed(e);
        } else {
          throw Throwables.propagate(e);
        }
      }
    }
  }
}

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

package io.cdap.plugin.db.batch.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.JDBCDriverShim;
import io.cdap.plugin.db.batch.NoOpCommitConnection;
import io.cdap.plugin.db.batch.TransactionIsolationLevel;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

/**
 * Class that extends {@link DBOutputFormat} to load the database driver class correctly.
 *
 * @param <K> - Key passed to this class to be written
 * @param <V> - Value passed to this class to be written. The value is ignored.
 */
public class ETLDBOutputFormat<K extends DBWritable, V> extends DBOutputFormat<K, V> {
  // Batch size before submitting a batch to the SQL engine. If set to 0, no batches will be submitted until commit.
  public static final String COMMIT_BATCH_SIZE = "io.cdap.plugin.db.output.commit.batch.size";
  public static final int DEFAULT_COMMIT_BATCH_SIZE = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(ETLDBOutputFormat.class);

  private Configuration conf;
  private Driver driver;
  private JDBCDriverShim driverShim;

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
    conf = context.getConfiguration();
    DBConfiguration dbConf = new DBConfiguration(conf);
    String tableName = dbConf.getOutputTableName();
    String[] fieldNames = dbConf.getOutputFieldNames();
    final int batchSize = conf.getInt(COMMIT_BATCH_SIZE, DEFAULT_COMMIT_BATCH_SIZE);

    if (fieldNames == null) {
      fieldNames = new String[dbConf.getOutputFieldCount()];
    }

    try {
      Connection connection = getConnection(conf);
      PreparedStatement statement = connection.prepareStatement(constructQuery(tableName, fieldNames));
      return new DBRecordWriter(connection, statement) {

        private boolean emptyData = true;
        private long numWrittenRecords = 0;

        //Implementation of the close method below is the exact implementation in DBOutputFormat except that
        //we check if there is any data to be written and if not, we skip executeBatch call.
        //There might be reducers that don't receive any data and thus this check is necessary to prevent
        //empty data to be committed (since some Databases doesn't support that).
        @Override
        public void close(TaskAttemptContext context) throws IOException {
          try {
            if (!emptyData) {
              getStatement().executeBatch();
              getConnection().commit();
            }
          } catch (SQLException e) {
            try {
              getConnection().rollback();
            } catch (SQLException ex) {
              LOG.warn(StringUtils.stringifyException(ex));
            }
            throw new IOException(e);
          } finally {
            try {
              getStatement().close();
              getConnection().close();
            } catch (SQLException ex) {
              throw new IOException(ex);
            }
          }

          try {
            DriverManager.deregisterDriver(driverShim);
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }

        @Override
        public void write(K key, V value) throws IOException {
          emptyData = false;
          //We need to make correct logging to avoid losing information about error
          try {
            key.write(getStatement());
            getStatement().addBatch();
            numWrittenRecords++;

            // Submit a batch to the SQL engine every 10k records
            // This is done to reduce memory usage in the worker, as processed records can now be GC'd.
            if (batchSize > 0 && numWrittenRecords % batchSize == 0) {
              getStatement().executeBatch();
            }
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }
      };
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  private Connection getConnection(Configuration conf) {
    Connection connection;
    try {
      String url = conf.get(DBConfiguration.URL_PROPERTY);
      try {
        // throws SQLException if no suitable driver is found
        DriverManager.getDriver(url);
      } catch (SQLException e) {
        if (driverShim == null) {
          if (driver == null) {
            ClassLoader classLoader = conf.getClassLoader();
            @SuppressWarnings("unchecked")
            Class<? extends Driver> driverClass =
              (Class<? extends Driver>) classLoader.loadClass(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
            driver = driverClass.newInstance();

            // De-register the default driver that gets registered when driver class is loaded.
            DBUtils.deregisterAllDrivers(driverClass);
          }

          driverShim = new JDBCDriverShim(driver);
          DriverManager.registerDriver(driverShim);
          LOG.debug("Registered JDBC driver via shim {}. Actual Driver {}.", driverShim, driver);
        }
      }

      ConnectionConfigAccessor connectionConfigAccessor = new ConnectionConfigAccessor(conf);
      Map<String, String> connectionArgs = connectionConfigAccessor.getConnectionArguments();
      Properties properties = new Properties();
      properties.putAll(connectionArgs);
      connection = DriverManager.getConnection(url, properties);

      boolean autoCommitEnabled = connectionConfigAccessor.isAutoCommitEnabled();
      if (autoCommitEnabled) {
        // hack to work around jdbc drivers like the hive driver that throw exceptions on commit
        connection = new NoOpCommitConnection(connection);
      } else {
        connection.setAutoCommit(false);
      }
      String level = connectionConfigAccessor.getTransactionIsolationLevel();
      LOG.debug("Transaction isolation level: {}", level);
      connection.setTransactionIsolation(TransactionIsolationLevel.getLevel(level));
      // execute initialization queries if any
      for (String query : connectionConfigAccessor.getInitQueries()) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(query);
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return connection;
  }

  @Override
  public String constructQuery(String table, String[] fieldNames) {
    String query = super.constructQuery(table, fieldNames);
    // Strip the ';' at the end since Oracle doesn't like it.
    // TODO: Perhaps do a conditional if we can find a way to tell that this is going to Oracle
    // However, tested this to work on Mysql and Oracle
    query = query.substring(0, query.length() - 1);

    String urlProperty = conf.get(DBConfiguration.URL_PROPERTY);
    if (urlProperty.startsWith("jdbc:phoenix")) {
      LOG.debug("Phoenix jdbc connection detected. Replacing INSERT with UPSERT.");
      Preconditions.checkArgument(query.startsWith("INSERT"), "Expecting query to start with 'INSERT'");
      query = "UPSERT" + query.substring("INSERT".length());
    }
    return query;
  }
}

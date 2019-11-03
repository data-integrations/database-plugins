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

package io.cdap.plugin.neo4j.sink;

import com.google.common.base.Throwables;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.JDBCDriverShim;
import io.cdap.plugin.neo4j.Neo4jConstants;
import io.cdap.plugin.neo4j.Neo4jRecord;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A OutputFormat that write data to Neo4j.
 *
 * @param <K> - Key passed to this class to be written
 * @param <V> - Value passed to this class to be written. The value is ignored.
 */
public class Neo4jDataDriveDBOutputFormat<K extends DBWritable, V> extends DBOutputFormat<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jDataDriveDBOutputFormat.class);

  private Configuration conf;
  private Driver driver;
  private JDBCDriverShim driverShim;

  private Map<Integer, String> positions = new HashMap<>();

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
    conf = context.getConfiguration();

    try {
      Connection connection = getConnection(conf);
      PreparedStatement statement = connection.prepareStatement(constructQuery());
      return new DBRecordWriter(connection, statement) {

        private boolean emptyData = true;

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
        public void write(K key, V value) {
          emptyData = false;
          //We need to make correct logging to avoid losing information about error
          try {
            ((Neo4jRecord) key).setPositions(positions);
            key.write(getStatement());
            getStatement().addBatch();
          } catch (SQLException e) {
            LOG.warn("Failed to write value to database", e);
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
      connection.setReadOnly(false);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return connection;
  }

  public String constructQuery() {
    return processQuery(conf.get(Neo4jConstants.OUTPUT_QUERY));
  }

  public String processQuery(String query) {
    positions = new HashMap<>();
    String regex = "\\$\\([^()]+\\)";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(query);
    int counter = 1;
    while (matcher.find()) {
      String group = matcher.group();
      positions.put(counter, group.substring(group.indexOf("(") + 1, group.indexOf(")")));
      query = query.replace(group, "{" + counter + "}");
      counter++;
    }
    return query;
  }
}

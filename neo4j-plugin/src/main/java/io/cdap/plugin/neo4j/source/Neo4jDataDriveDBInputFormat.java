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

package io.cdap.plugin.neo4j.source;

import com.google.common.base.Throwables;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.JDBCDriverShim;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A InputFormat that reads input data from an Neo4j.
 *
 * @param <T>
 */
public class Neo4jDataDriveDBInputFormat<T extends DBWritable> extends DBInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jDataDriveDBInputFormat.class);

  private Driver driver;
  private JDBCDriverShim driverShim;

  public static void setInput(Configuration conf, Class<? extends DBWritable> inputClass, String inputQuery,
                              String orderBy, boolean enableAutoCommit) {
    DBConfiguration dbConf = new DBConfiguration(conf);
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputOrderBy(orderBy);
    new ConnectionConfigAccessor(conf).setAutoCommitEnabled(enableAutoCommit);
  }

  @Override
  public void setConf(Configuration conf) {
    dbConf = new DBConfiguration(conf);
    connection = getConnection();
    tableName = dbConf.getInputTableName();
    fieldNames = dbConf.getInputFieldNames();
    conditions = dbConf.getInputConditions();
  }

  @Override
  public Connection getConnection() {
    if (this.connection == null) {
      ConnectionConfigAccessor connectionConfigAccessor = new ConnectionConfigAccessor(getConf());
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
        connection = DriverManager.getConnection(url, properties);
        connection.setReadOnly(true);
        connection.setAutoCommit(connectionConfigAccessor.isAutoCommitEnabled());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return this.connection;
  }

  @Override
  protected RecordReader createDBRecordReader(DBInputFormat.DBInputSplit split,
                                              Configuration conf) throws IOException {
    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    try {
      // Use Neo4j-specific db reader
      return new Neo4jDBRecordReader<>(split, inputClass, conf, getConnection(), dbConf,
                                       dbConf.getInputConditions(), dbConf.getInputFieldNames(),
                                       dbConf.getInputTableName());
    } catch (SQLException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  @Override
  protected String getCountQuery() {
    String query = dbConf.getInputQuery();
    StringBuilder sb = new StringBuilder();
    for (String s : query.split(" ")) {
      if (s.toUpperCase().equals("RETURN")) {
        sb.append(query, 0, query.indexOf(s)).append("RETURN COUNT(*)");
        break;
      }
    }
    return sb.toString();
  }
}

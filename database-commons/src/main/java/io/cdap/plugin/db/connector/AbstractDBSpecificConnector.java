/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.plugin.db.connector;

import com.google.common.collect.Maps;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.connector.SampleType;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.db.AbstractDBConnector;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.common.util.ExceptionUtils;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.source.DataDrivenETLDBInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * An Abstract DB Specific Connector those specific DB connectors can inherits
 *
 * @param <T> the Record type that specific DB Record Reader may return while sample the data with InputFormat
 */
public abstract class AbstractDBSpecificConnector<T extends DBWritable> extends AbstractDBConnector
  implements BatchConnector<LongWritable, T> {

  private final AbstractDBConnectorConfig config;

  protected AbstractDBSpecificConnector(AbstractDBConnectorConfig config) {
    super(config);
    this.config = config;
  }

  public abstract boolean supportSchema();

  protected abstract Class<? extends DBWritable> getDBRecordType();

  protected SchemaReader getSchemaReader(String sessionID) {
    return new CommonSchemaReader();
  }

  @Override
  protected DBConnectorPath getDBConnectorPath(String path) throws IOException {
    return DBSpecificPath.of(path, supportSchema());
  }

  @Override
  public InputFormatProvider getInputFormatProvider(ConnectorContext context, SampleRequest request)
    throws IOException {
    DBConnectorPath path = getDBConnectorPath(request.getPath());
    if (path.getTable() == null) {
      throw new IllegalArgumentException(
        String.format("Path %s cannot be sampled. Must have table name in the path.", request.getPath()));
    }

    ConnectionConfigAccessor connectionConfigAccessor = new ConnectionConfigAccessor();
    if (config.getUser() == null && config.getPassword() == null) {
      DBConfiguration.configureDB(connectionConfigAccessor.getConfiguration(), driverClass.getName(),
                                  getConnectionString(path.getDatabase()));
    } else {
      DBConfiguration.configureDB(connectionConfigAccessor.getConfiguration(), driverClass.getName(),
                                  getConnectionString(path.getDatabase()), config.getUser(), config.getPassword());
    }
    String sessionID = generateSessionID();
    String tableQuery = getTableQuery(path.getDatabase(), path.getSchema(), path.getTable(), request.getLimit(),
      request.getProperties().get("sampleType"), request.getProperties().get("strata"), sessionID);
    DataDrivenETLDBInputFormat.setInput(connectionConfigAccessor.getConfiguration(), getDBRecordType(),
      tableQuery, null, false);
    connectionConfigAccessor.setConnectionArguments(Maps.fromProperties(config.getConnectionArgumentsProperties()));
    connectionConfigAccessor.getConfiguration().setInt(MRJobConfig.NUM_MAPS, 1);
    Map<String, String> additionalArguments = config.getAdditionalArguments();
    for (Map.Entry<String, String> argument : additionalArguments.entrySet()) {
      connectionConfigAccessor.getConfiguration().set(argument.getKey(), argument.getValue());
    }
    try {
      Long timeoutMs = request.getTimeoutMs();
      Integer timeoutSec = timeoutMs != null ? (int) (timeoutMs / 1000) : null;
      connectionConfigAccessor
        .setSchema(loadTableSchema(getConnection(path), tableQuery, timeoutSec, sessionID).toString());
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to get table schema due to: %s.",
        ExceptionUtils.getRootCauseMessage(e)), e);
    }

    return new SourceInputFormatProvider(DataDrivenETLDBInputFormat.class, connectionConfigAccessor.getConfiguration());
  }

  protected Connection getConnection(DBConnectorPath path) {
    return getConnection(getConnectionString(path.getDatabase()), config.getConnectionArgumentsProperties());
  }

  protected String getConnectionString(String database) {
    return config.getConnectionString();
  }

  protected String getTableName(String database, String schema, String table) {
    return schema == null ? String.format("\"%s\".\"%s\"", database, table)
      : String.format("\"%s\".\"%s\".\"%s\"", database, schema, table);
  }

  protected String getTableQuery(String database, String schema, String table) {
    String tableName = getTableName(database, schema, table);
    return String.format("SELECT * FROM %s", tableName);
  }

  protected String getTableQuery(String database, String schema, String table, int limit) {
    String tableName = getTableName(database, schema, table);
    return String.format("SELECT * FROM %s LIMIT %d", tableName, limit);
  }

  protected String getTableQuery(String database, String schema, String table, int limit, String sampleType,
                                 String strata, String sessionID) throws IOException {
    if (sampleType == null) {
      return getTableQuery(database, schema, table, limit);
    }
    String tableName = getTableName(database, schema, table);
    switch (SampleType.fromString(sampleType)) {
      case RANDOM:
        return getRandomQuery(tableName, limit);
      case STRATIFIED:
        if (strata == null) {
          throw new IllegalArgumentException("No strata column given.");
        }
        return getStratifiedQuery(tableName, limit, strata, sessionID);
      default:
        return getTableQuery(database, schema, table, limit);
    }
  }

  // Get the query to use for randomized sampling.
  // By default, databases don't support randomized sampling; this method must be overridden
  protected String getRandomQuery(String tableName, int limit) throws IOException {
    throw new IOException("Connection does not support random sampling.");
  }

  // Get the query to use for stratified sampling.
  // By default, databases don't support stratified sampling; this method must be overridden
  protected String getStratifiedQuery(String tableName, int limit, String strata, String sessionID) throws IOException {
    throw new IOException("Connection does not support stratified sampling.");
  }

  protected Schema loadTableSchema(Connection connection, String query, @Nullable Integer timeoutSec, String sessionID)
    throws SQLException {
    Statement statement = connection.createStatement();
    statement.setMaxRows(1);
    if (timeoutSec != null) {
      statement.setQueryTimeout(timeoutSec);
    }
    ResultSet resultSet = statement.executeQuery(query);
    return Schema.recordOf("outputSchema", getSchemaReader(sessionID).getSchemaFields(resultSet));
  }

  protected void setConnectionProperties(Map<String, String> properties, ConnectorSpecRequest request) {
    properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, request.getConnectionWithMacro());
  }

  @Override
  protected Schema getTableSchema(Connection connection, String database,
                                  String schema, String table) throws SQLException {
    String sessionID = generateSessionID();
    return loadTableSchema(getConnection(), getTableQuery(database, schema, table),
      null, sessionID);
  }

  protected String generateSessionID() {
    return UUID.randomUUID().toString().replace('-', '_');
  }
}

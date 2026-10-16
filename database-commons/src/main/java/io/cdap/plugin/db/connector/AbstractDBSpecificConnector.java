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
import dev.failsafe.RetryPolicy;
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
import io.cdap.plugin.common.db.DBErrorDetailsProvider;
import io.cdap.plugin.common.util.ExceptionUtils;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.ReadOnlyConnection;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.source.DataDrivenETLDBInputFormat;
import io.cdap.plugin.util.RetryPolicyUtil;
import io.cdap.plugin.util.RetryUtils;
import io.cdap.plugin.util.SqlIdentifierUtils;
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
import java.util.Properties;
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
  private final RetryPolicy<?> retryPolicy;
  protected DBErrorDetailsProvider dbErrorDetailsProvider;

  protected AbstractDBSpecificConnector(AbstractDBConnectorConfig config) {
    super(config);
    this.config = config;
    this.retryPolicy = RetryPolicyUtil.getRetryPolicy(config.getInitialRetryDuration(), config.getMaxRetryDuration(),
      config.getMaxRetryCount());
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
    Long timeoutMs = request.getTimeoutMs();
    Integer timeoutSec = timeoutMs != null ? (int) (timeoutMs / 1000) : null;
    String tableQuery;
    try (Connection connection = getConnection(path)) {
      // Make sure the table really exists before any query embedding the (user supplied) identifier is built or
      // executed. The identifiers are quoted/escaped by getTableName(), this check is defense in depth.
      validateTable(connection, path.getDatabase(), path.getSchema(), path.getTable());
      tableQuery = getTableQuery(path.getDatabase(), path.getSchema(), path.getTable(), request.getLimit(),
        request.getProperties().get("sampleType"), request.getProperties().get("strata"), sessionID);
      connectionConfigAccessor.setSchema(loadTableSchema(connection, tableQuery, timeoutSec, sessionID).toString());
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to get table schema due to: %s.",
        ExceptionUtils.getRootCauseMessage(e)), e);
    }

    DataDrivenETLDBInputFormat.setInput(connectionConfigAccessor.getConfiguration(), getDBRecordType(),
      tableQuery, null, false);
    // Sampling only ever reads: open the input format connection in read-only, non auto-commit mode.
    connectionConfigAccessor.setReadOnly(true);
    connectionConfigAccessor.setConnectionArguments(Maps.fromProperties(config.getConnectionArgumentsProperties()));
    connectionConfigAccessor.getConfiguration().setInt(MRJobConfig.NUM_MAPS, 1);
    Map<String, String> additionalArguments = config.getAdditionalArguments();
    for (Map.Entry<String, String> argument : additionalArguments.entrySet()) {
      connectionConfigAccessor.getConfiguration().set(argument.getKey(), argument.getValue());
    }

    return new SourceInputFormatProvider(DataDrivenETLDBInputFormat.class, connectionConfigAccessor.getConfiguration());
  }

  /**
   * Returns the DBErrorDetailsProvider instance.
   * Override this method to provide a custom DBErrorDetailsProvider instance.
   *
   * @return DBErrorDetailsProvider instance
   */
  protected DBErrorDetailsProvider getErrorDetailsProvider() {
    if (dbErrorDetailsProvider == null) {
      dbErrorDetailsProvider = new DBErrorDetailsProvider();
    }
    return dbErrorDetailsProvider;
  }

  protected Connection getConnection(DBConnectorPath path) {
    return getConnection(getConnectionString(path.getDatabase()), config.getConnectionArgumentsProperties());
  }

  /**
   * Opens a connection for connector operations (test, browse, spec generation and sampling schema discovery).
   * None of these operations ever write to the database, so the connection is returned in read-only, non
   * auto-commit mode (see {@link ReadOnlyConnection}); any DML/DDL that reaches the database through it is rejected
   * and nothing can ever be committed.
   */
  @Override
  protected Connection getConnection(String connectionString, Properties connectionArguments) {
    Connection connection = super.getConnection(connectionString, connectionArguments);
    try {
      return ReadOnlyConnection.wrap(connection);
    } catch (SQLException e) {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException closeEx) {
          e.addSuppressed(closeEx);
        }
      }
      throw new IllegalArgumentException(String.format(
        "Failed to open a read-only connection to database via connection string: %s. Error: %s.",
        connectionString, ExceptionUtils.getRootCauseMessage(e)), e);
    }
  }

  protected String getConnectionString(String database) {
    return config.getConnectionString();
  }

  /**
   * Validates that the given table (or view) exists according to the JDBC metadata, mirroring the database and schema
   * validations done while browsing. This must be called before any query embedding the table identifier is executed.
   */
  protected void validateTable(Connection connection, @Nullable String database, @Nullable String schema,
                               String table) throws SQLException {
    try (ResultSet tables = connection.getMetaData().getTables(database, schema, table, null)) {
      if (!tables.next()) {
        throw new IllegalArgumentException(
          schema == null ? String.format("Table '%s' does not exist.", table)
            : String.format("Table '%s' does not exist in schema '%s'.", table, schema));
      }
    }
  }

  /**
   * Returns the fully qualified, quoted table name to be used in queries. Every identifier is quoted and escaped so
   * that user supplied path components can never break out of the identifier.
   */
  protected String getTableName(String database, String schema, String table) {
    String quotedDatabase = SqlIdentifierUtils.quoteAnsiIdentifier(database);
    String quotedTable = SqlIdentifierUtils.quoteAnsiIdentifier(table);
    return schema == null ? quotedDatabase + "." + quotedTable
      : quotedDatabase + "." + SqlIdentifierUtils.quoteAnsiIdentifier(schema) + "." + quotedTable;
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
      try (Statement statement = RetryUtils.createStatementWithRetry((RetryPolicy<Statement>) retryPolicy, connection,
        getErrorDetailsProvider())) {
        statement.setMaxRows(1);
        if (timeoutSec != null) {
          statement.setQueryTimeout(timeoutSec);
        }
        ResultSet resultSet = RetryUtils.executeQueryWithRetry((RetryPolicy<ResultSet>) retryPolicy, statement, query,
          getErrorDetailsProvider());
        return Schema.recordOf("outputSchema", getSchemaReader(sessionID).getSchemaFields(resultSet));
    }
  }

  protected void setConnectionProperties(Map<String, String> properties, ConnectorSpecRequest request) {
    properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, request.getConnectionWithMacro());
  }

  /**
   * Called by {@code AbstractDBConnector#generateSpec} after the database and schema have been validated on the
   * given connection. The table is validated here as well, before the schema discovery query is executed.
   */
  @Override
  protected Schema getTableSchema(Connection connection, String database,
                                  String schema, String table) throws SQLException {
    validateTable(connection, database, schema, table);
    String sessionID = generateSessionID();
    return loadTableSchema(connection, getTableQuery(database, schema, table), null, sessionID);
  }

  protected String generateSessionID() {
    return UUID.randomUUID().toString().replace('-', '_');
  }
}

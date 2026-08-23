/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.databricks;

import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.db.NoOpCommitConnection;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.TransactionIsolationLevel;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import io.cdap.plugin.db.connector.DBSpecificPath;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Databricks Database Connector that connects to Databricks database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(DatabricksConstants.PLUGIN_NAME)
@Description("Connection to access data in Databricks using JDBC.")
@Category("Database")
public class DatabricksConnector extends AbstractDBSpecificConnector<DatabricksDBRecord> {
  public static final String NAME = DatabricksConstants.PLUGIN_NAME;
  private final DatabricksConnectorConfig config;

  private static final Logger LOG = LoggerFactory.getLogger(DatabricksConnector.class);

  public DatabricksConnector(DatabricksConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected DBConnectorPath getDBConnectorPath(String path) throws IOException {
    return DBSpecificPath.of(path, supportSchema());
  }

  @Override
  protected Connection getConnection(DBConnectorPath path) {
    Connection connection = super.getConnection(path);
    try {
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    } catch (SQLException e) {
      LOG.warn("Failed to set transaction isolation level to READ_UNCOMMITTED", e);
    }
    return new NoOpCommitConnection(connection);
  }

  @Override
  protected Connection getConnection() {
    Connection connection = super.getConnection();
    try {
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    } catch (SQLException e) {
      LOG.warn("Failed to set transaction isolation level to READ_UNCOMMITTED", e);
    }
    return new NoOpCommitConnection(connection);
  }

  @Override
  public boolean supportSchema() {
    return true;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return DatabricksDBRecord.class;
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, DatabricksDBRecord record) {
    return record.getRecord();
  }

  @Override
  protected SchemaReader getSchemaReader(String sessionID) {
    return new DatabricksSchemaReader(sessionID);
  }

  @Override
  protected String getTableName(String database, String schema, String table) {
    if (database == null && schema == null) {
      return String.format("`%s`", table);
    }
    if (database == null) {
      return String.format("`%s`.`%s`", schema, table);
    }
    if (schema == null) {
      return String.format("`%s`.`%s`", database, table);
    }
    return String.format("`%s`.`%s`.`%s`", database, schema, table);
  }

  @Override
  protected String getRandomQuery(String tableName, int limit) {
    return String.format("SELECT * FROM %s LIMIT %d", tableName, limit);
  }

  @Override
  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> sourceProperties = new HashMap<>();
    setConnectionProperties(sourceProperties, request);
    builder.addRelatedPlugin(new PluginSpec(DatabricksConstants.PLUGIN_NAME,
                                            BatchSource.PLUGIN_TYPE, sourceProperties));

    String schema = path.getSchema();
    sourceProperties.put(DatabricksSource.DatabricksSourceConfig.NUM_SPLITS, "1");
    sourceProperties.put(DatabricksSource.DatabricksSourceConfig.FETCH_SIZE,
                         DatabricksSource.DatabricksSourceConfig.DEFAULT_FETCH_SIZE);
    String table = path.getTable();
    if (table == null) {
      return;
    }
    sourceProperties.put(DatabricksSource.DatabricksSourceConfig.IMPORT_QUERY,
                         getTableQuery(path.getDatabase(), schema, table));
    sourceProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
  }

  @Override
  protected boolean isAutoCommitEnabled() {
    return true;
  }

  @Override
  protected String getTransactionIsolationLevel() {
    return TransactionIsolationLevel.Level.TRANSACTION_READ_UNCOMMITTED.name();
  }
}

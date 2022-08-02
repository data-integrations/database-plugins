/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.mssql;

import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A SQL Server Database Connector that connects to SQL Server database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(SqlServerConnector.NAME)
@Description("Connection to access data in SQL Server databases using JDBC.")
@Category("Database")
public class SqlServerConnector extends AbstractDBSpecificConnector<SqlServerSourceDBRecord> {
  public static final String NAME = "SQL Server";
  private final SqlServerConnectorConfig config;

  public SqlServerConnector(SqlServerConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public boolean supportSchema() {
    return true;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return SqlServerSourceDBRecord.class;
  }

  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> sourceProperties = new HashMap<>();
    Map<String, String> sinkProperties = new HashMap<>();
    setConnectionProperties(sourceProperties, request);
    setConnectionProperties(sinkProperties, request);
    builder
      .addRelatedPlugin(new PluginSpec(SqlServerConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties))
      .addRelatedPlugin(new PluginSpec(SqlServerConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProperties));

    String database = path.getDatabase();
    if (database != null) {
      sinkProperties.put(ConnectionConfig.DATABASE, database);
      sourceProperties.put(ConnectionConfig.DATABASE, database);
    }
    String schema = path.getSchema();
    if (schema != null) {
      sinkProperties.put(SqlServerSink.SqlServerSinkConfig.DB_SCHEMA_NAME, schema);
    }
    sourceProperties.put(SqlServerSource.SqlServerSourceConfig.NUM_SPLITS, "1");
    sourceProperties.put(SqlServerSource.SqlServerSourceConfig.FETCH_SIZE,
                         SqlServerSource.SqlServerSourceConfig.DEFAULT_FETCH_SIZE);
    String table = path.getTable();
    if (table == null) {
      return;
    }
    sourceProperties.put(SqlServerSource.SqlServerSourceConfig.IMPORT_QUERY,
                         getTableQuery(database, schema, table));
    sinkProperties.put(SqlServerSink.SqlServerSinkConfig.TABLE_NAME, table);
    sourceProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
    sinkProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSourceSchemaReader();
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, SqlServerSourceDBRecord record) {
    return record.getRecord();
  }

  @Override
  protected String getTableQuery(String database, String schema, String table, int limit) {
    String tableName = getTableName(database, schema, table);
    return String.format(
      "SELECT TOP %d * FROM %s", limit, tableName);
  }

  @Override
  protected Schema getSchema(int sqlType, String typeName, int scale, int precision, String columnName,
                             boolean isSigned, boolean handleAsDecimal) throws SQLException {
    if (SqlServerSourceSchemaReader.shouldConvertToDatetime(typeName)) {
      return Schema.of(Schema.LogicalType.DATETIME);
    }

    if (SqlServerSourceSchemaReader.GEOMETRY_TYPE == sqlType || SqlServerSourceSchemaReader.GEOGRAPHY_TYPE == sqlType) {
      return Schema.of(Schema.Type.BYTES);
    }
    return super.getSchema(sqlType, typeName, scale, precision, columnName, isSigned, handleAsDecimal);
  }

  @Override
  protected String getTableQuery(String database, String schema, String table, int limit, String sampleType,
                                 String strata) {
    if (sampleType == null) {
      return getTableQuery(database, schema, table, limit);
    }
    String tableName = getTableName(database, schema, table);
    switch (sampleType) {
      case "random":
        // This query doesn't guarantee exactly "limit" number of rows
        return String.format("SELECT * FROM %s " +
                "WHERE (ABS(CAST((BINARY_CHECKSUM(*) * RAND()) as int)) %% 100) " +
                "< %d / (SELECT COUNT(*) FROM %s)", tableName, limit * 100, tableName);

        // Alternative method, which is quicker and almost guarantees the correct number of rows,
        // but isn't uniformly random (i.e. rows are usually clustered together)
        // return String.format("SELECT TOP %d * FROM %s TABLESAMPLE(%d ROWS)", limit, tableName, limit*2);
       case "stratified":
        if (strata == null) {
          throw new IllegalArgumentException("No strata column given.");
        }
         return String.format("SELECT * FROM %s " +
                 "WHERE (ABS(CAST((BINARY_CHECKSUM(*) * RAND()) as int)) %% 100) " +
                 "< %d / (SELECT COUNT(*) FROM %s)" +
                 "ORDER BY %s", tableName, limit * 100, tableName, strata);
      // TODO: add in stratified sampling here
      default:
        return super.getTableQuery(database, schema, table, limit);
    }
  }
}

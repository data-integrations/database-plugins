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

package io.cdap.plugin.oracle;

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
import io.cdap.cdap.etl.api.connector.SampleType;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.common.db.DBPath;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * A Oracle Database Connector that connects to Oracle database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(OracleConnector.NAME)
@Description("Connection to access data in Oracle databases using JDBC.")
@Category("Database")
public class OracleConnector extends AbstractDBSpecificConnector<OracleSourceDBRecord> {
  public static final String NAME = "Oracle";

  private final OracleConnectorConfig config;

  public OracleConnector(OracleConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public boolean supportSchema() {
    return true;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return OracleSourceDBRecord.class;
  }

  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> sourceProperties = new HashMap<>();
    Map<String, String> sinkProperties = new HashMap<>();
    setConnectionProperties(sourceProperties, request);
    setConnectionProperties(sinkProperties, request);
    builder
      .addRelatedPlugin(new PluginSpec(OracleConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties))
      .addRelatedPlugin(new PluginSpec(OracleConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProperties))
      .addSupportedSampleType(SampleType.RANDOM)
      .addSupportedSampleType(SampleType.STRATIFIED);

    String schema = path.getSchema();
    if (schema != null) {
      sinkProperties.put(OracleSink.OracleSinkConfig.DB_SCHEMA_NAME, schema);
    }
    sourceProperties.put(OracleSource.OracleSourceConfig.NUM_SPLITS, "1");
    sourceProperties.put(OracleSource.OracleSourceConfig.FETCH_SIZE,
                         OracleSource.OracleSourceConfig.DEFAULT_FETCH_SIZE);
    sourceProperties.put(OracleConstants.DEFAULT_ROW_PREFETCH,
                         OracleSource.OracleSourceConfig.DEFAULT_ROW_PREFETCH_VALUE);
    sourceProperties.put(OracleConstants.DEFAULT_BATCH_VALUE,
                         OracleSource.OracleSourceConfig.DEFAULT_BATCH_SIZE);
    String table = path.getTable();
    if (table == null) {
      return;
    }
    sourceProperties.put(OracleSource.OracleSourceConfig.IMPORT_QUERY,
                         getTableQuery(path.getDatabase(), schema, table));
    sinkProperties.put(OracleSink.OracleSinkConfig.TABLE_NAME, table);
    sourceProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
    sinkProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
  }

  @Override
  protected DBConnectorPath getDBConnectorPath(String path) {
    return new DBPath(path, true);
  }

  @Override
  protected SchemaReader getSchemaReader(String sessionID) {
    return new OracleSourceSchemaReader(sessionID);
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, OracleSourceDBRecord record) {
    return record.getRecord();
  }

  @Override
  protected String getConnectionString(@Nullable String database) {
    if (database == null) {
      return config.getConnectionString();
    }
    if (OracleConstants.TNS_CONNECTION_TYPE.equals(config.getConnectionType())) {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_TNS_FORMAT, database);
    } else if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(config.getConnectionType())) {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SERVICE_NAME_FORMAT, config.getHost(),
              config.getPort(), database);
    } else {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SID_FORMAT,
              config.getHost(), config.getPort(), database);
    }
  }

  @Override
  protected String getTableName(String database, String schema, String table) {
    return String.format("\"%s\".\"%s\"", schema, table);
  }

  @Override
  protected String getTableQuery(String database, String schema, String table, int limit) {
    String tableName = getTableName(database, schema, table);
    return String.format("SELECT * FROM %s WHERE ROWNUM <= %d", tableName, limit);
  }

  @Override
  protected String getRandomQuery(String tableName, int limit) {
    // This query guarantees exactly "limit" rows.
    // Note that it is very slow on large tables, since it assigns _every_ row a number and then sorts them
    return String.format("SELECT * FROM (\n" +
                           "SELECT * FROM %s ORDER BY DBMS_RANDOM.RANDOM\n" +
                           ")\n" +
                           "WHERE rownum <= %d",
                         tableName, limit);
  }

  @Override
  protected String getStratifiedQuery(String tableName, int limit, String strata, String sessionID) {
    return String.format("WITH t_%s AS (\n" +
        "    SELECT %s.*,\n" +
        "    ROW_NUMBER() OVER (ORDER BY %s, dbms_random.random) AS s_%s,\n" +
        "    (SELECT COUNT(*) FROM %s) AS c_%s\n" +
        "    FROM %s\n" +
        "    ORDER BY %s" +
        "  )\n" +
        "SELECT * FROM t_%s\n" +
        "WHERE MOD(s_%s, GREATEST(1, CAST(c_%s / %d AS INTEGER))) = 1 AND ROWNUM <= %d",
      sessionID, tableName, strata, sessionID, tableName, sessionID, tableName,
      strata, sessionID, sessionID, sessionID, limit, limit);
  }

  @Override
  protected Schema getSchema(int sqlType, String typeName, int scale, int precision, String columnName,
                             boolean isSigned, boolean handleAsDecimal) throws SQLException {
    // For a Number type without specified precision and scale, precision will be 0 and scale will be -127
    if (precision == 0) {
      // reference : https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
      precision = 38;
      scale = 0;
    }
    return super.getSchema(sqlType, typeName, scale, precision, columnName, isSigned, handleAsDecimal);
  }

  @Override
  protected String generateSessionID() {
    // SessionID cannot be greater than 28 characters, as it is used as a suffix for variable names,
    // and Oracle does not allow variable names over 30 characters.
    // Thus sessionID is not a "true" UUID, but the chance of collision is still negligible.
    return UUID.randomUUID()
      .toString()
      .replaceAll("-", "")
      .substring(0, 28);
  }
}

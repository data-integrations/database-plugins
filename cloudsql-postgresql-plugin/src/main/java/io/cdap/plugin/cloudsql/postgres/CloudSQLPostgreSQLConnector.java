/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.cloudsql.postgres;

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
import io.cdap.plugin.common.db.DBPath;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import io.cdap.plugin.postgres.PostgresDBRecord;
import io.cdap.plugin.postgres.PostgresSchemaReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A CloudSQL PostgreSQL Database Connector that connects to CloudSQL PostgreSQL database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(CloudSQLPostgreSQLConnector.NAME)
@Description("Connection to access data in CloudSQL PostgreSQL Server databases using JDBC.")
@Category("Database")
public class CloudSQLPostgreSQLConnector extends AbstractDBSpecificConnector<PostgresDBRecord> {
  public static final String NAME = CloudSQLPostgreSQLConstants.PLUGIN_NAME;
  private final CloudSQLPostgreSQLConnectorConfig config;

  public CloudSQLPostgreSQLConnector(CloudSQLPostgreSQLConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected DBConnectorPath getDBConnectorPath(String path) throws IOException {
    return new DBPath(path, true);
  }

  @Override
  public boolean supportSchema() {
    return true;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return PostgresDBRecord.class;
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, PostgresDBRecord postgresDBRecord) {
    return postgresDBRecord.getRecord();
  }

  @Override
  protected List<Schema.Field> getSchemaFields(ResultSet resultSet, String sessionID) throws SQLException {
    return new PostgresSchemaReader(sessionID).getSchemaFields(resultSet);
  }

  @Override
  protected String getTableName(String database, String schema, String table) {
    return String.format("\"%s\".\"%s\"", schema, table);
  }

  @Override
  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> sourceProperties = new HashMap<>();
    Map<String, String> sinkProperties = new HashMap<>();
    setConnectionProperties(sourceProperties, request);
    setConnectionProperties(sinkProperties, request);
    builder
      .addRelatedPlugin(new PluginSpec(CloudSQLPostgreSQLConstants.PLUGIN_NAME,
                                       BatchSource.PLUGIN_TYPE, sourceProperties))
      .addRelatedPlugin(new PluginSpec(CloudSQLPostgreSQLConstants.PLUGIN_NAME,
                                       BatchSink.PLUGIN_TYPE, sinkProperties));

    String schema = path.getSchema();
    if (schema != null) {
      sinkProperties.put(CloudSQLPostgreSQLSink.CloudSQLPostgreSQLSinkConfig.DB_SCHEMA_NAME, schema);
    }
    sourceProperties.put(CloudSQLPostgreSQLSource.CloudSQLPostgreSQLSourceConfig.NUM_SPLITS, "1");
    sourceProperties.put(CloudSQLPostgreSQLSource.CloudSQLPostgreSQLSourceConfig.FETCH_SIZE,
                         CloudSQLPostgreSQLSource.CloudSQLPostgreSQLSourceConfig.DEFAULT_FETCH_SIZE);
    String table = path.getTable();
    if (table == null) {
      return;
    }
    sourceProperties.put(CloudSQLPostgreSQLSource.CloudSQLPostgreSQLSourceConfig.IMPORT_QUERY,
                         getTableQuery(path.getDatabase(), schema, table));
    sinkProperties.put(CloudSQLPostgreSQLSink.CloudSQLPostgreSQLSinkConfig.TABLE_NAME, table);
    sourceProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
    sinkProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
  }
}

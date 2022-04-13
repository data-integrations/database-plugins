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

package io.cdap.plugin.postgres;

import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
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
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A Postgre SQL Database Connector that connects to PostgreSQL database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(PostgresConnector.NAME)
@Description("Connection to access data in PostgreSQL databases using JDBC.")
@Category("Database")
public class PostgresConnector extends AbstractDBSpecificConnector<PostgresDBRecord> {
  public static final String NAME = "PostgreSQL";
  private final PostgresConnectorConfig config;

  public PostgresConnector(PostgresConnectorConfig config) {
    super(config);
    this.config = config;
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
  protected DBConnectorPath getDBConnectorPath(String path) throws IOException {
      return new DBPath(path, true);
  }

  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> sourceProperties = new HashMap<>();
    Map<String, String> sinkProperties = new HashMap<>();
    setConnectionProperties(sourceProperties, request);
    setConnectionProperties(sinkProperties, request);
    builder
      .addRelatedPlugin(new PluginSpec(PostgresConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties))
      .addRelatedPlugin(new PluginSpec(PostgresConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProperties));

    sinkProperties.put(PostgresConnectorConfig.NAME_DATABASE, config.getDatabase());
    String schema = path.getSchema();
    if (schema != null) {
      sinkProperties.put(PostgresSink.PostgresSinkConfig.DB_SCHEMA_NAME, schema);
    }
    sourceProperties.put(PostgresSource.PostgresSourceConfig.NUM_SPLITS, "1");
    String table = path.getTable();
    if (table == null) {
      return;
    }
    sourceProperties.put(PostgresSource.PostgresSourceConfig.IMPORT_QUERY,
                         getTableQuery(path.getDatabase(), schema, table));
    sinkProperties.put(PostgresSink.PostgresSinkConfig.TABLE_NAME, table);
    sourceProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
    sinkProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new PostgresSchemaReader();
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, PostgresDBRecord record) {
    return record.getRecord();
  }

  @Override
  protected String getTableQuery(String database, String schema, String table) {
    return String.format("SELECT * FROM \"%s\".\"%s\"", schema, table);
  }

  @Override
  protected String getTableQuery(String database, String schema, String table, int limit) {
    return String.format("SELECT * FROM \"%s\".\"%s\" LIMIT %d", schema, table, limit);
  }

}

/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.amazon.redshift;

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
import io.cdap.plugin.common.db.DBPath;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Amazon Redshift Database Connector that connects to Amazon Redshift database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(RedshiftConnector.NAME)
@Description("Connection to access data in Amazon Redshift using JDBC.")
@Category("Database")
public class RedshiftConnector extends AbstractDBSpecificConnector<io.cdap.plugin.amazon.redshift.RedshiftDBRecord> {
  public static final String NAME = RedshiftConstants.PLUGIN_NAME;
  private final RedshiftConnectorConfig config;

  public RedshiftConnector(RedshiftConnectorConfig config) {
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
    return RedshiftDBRecord.class;
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, RedshiftDBRecord redshiftDBRecord) {
    return redshiftDBRecord.getRecord();
  }

  @Override
  protected SchemaReader getSchemaReader(String sessionID) {
    return new RedshiftSchemaReader(sessionID);
  }

  @Override
  protected String getTableName(String database, String schema, String table) {
    return String.format("\"%s\".\"%s\"", schema, table);
  }

  @Override
  protected String getRandomQuery(String tableName, int limit) {
    return String.format("SELECT * FROM %s\n" +
                           "TABLESAMPLE BERNOULLI (100.0 * %d / (SELECT COUNT(*) FROM %s))",
                         tableName, limit, tableName);
  }

  @Override
  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> sourceProperties = new HashMap<>();
    setConnectionProperties(sourceProperties, request);
    builder
      .addRelatedPlugin(new PluginSpec(RedshiftConstants.PLUGIN_NAME,
                                       BatchSource.PLUGIN_TYPE, sourceProperties));

    String schema = path.getSchema();
    sourceProperties.put(RedshiftSource.RedshiftSourceConfig.NUM_SPLITS, "1");
    sourceProperties.put(RedshiftSource.RedshiftSourceConfig.FETCH_SIZE,
                         RedshiftSource.RedshiftSourceConfig.DEFAULT_FETCH_SIZE);
    String table = path.getTable();
    if (table == null) {
      return;
    }
    sourceProperties.put(RedshiftSource.RedshiftSourceConfig.IMPORT_QUERY,
                         getTableQuery(path.getDatabase(), schema, table));
    sourceProperties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
  }

}

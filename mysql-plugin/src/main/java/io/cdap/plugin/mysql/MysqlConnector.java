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

package io.cdap.plugin.mysql;

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
import io.cdap.plugin.common.db.DBRecord;
import io.cdap.plugin.common.db.schemareader.MysqlSchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Mysql Database Connector that connects to Mysql database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(MysqlConnector.NAME)
@Description("Connection to access data in Mysql databases using JDBC.")
@Category("Database")
public class MysqlConnector extends AbstractDBSpecificConnector<DBRecord> {
  public static final String NAME = "MySQL";
  private final MysqlConnectorConfig config;

  public MysqlConnector(MysqlConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public boolean supportSchema() {
    return false;
  }

  @Override
  protected List<Schema.Field> getSchemaFields(ResultSet resultSet, String sessionID) throws SQLException {
    return new MysqlSchemaReader(sessionID).getSchemaFields(resultSet, null, null);
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return MysqlDBRecord.class;
  }

  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> properties = new HashMap<>();
    setConnectionProperties(properties, request);
    builder
      .addRelatedPlugin(new PluginSpec(MysqlConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, properties))
      .addRelatedPlugin(new PluginSpec(MysqlConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, properties))
      .addSupportedSampleType(SampleType.RANDOM)
      .addSupportedSampleType(SampleType.STRATIFIED);

    String table = path.getTable();
    if (table == null) {
      return;
    }

    properties.put(MysqlSource.MysqlSourceConfig.IMPORT_QUERY, getTableQuery(path.getDatabase(), path.getSchema(),
                                                                             path.getTable()));
    properties.put(MysqlSource.MysqlSourceConfig.NUM_SPLITS, "1");
    properties.put(MysqlSource.MysqlSourceConfig.FETCH_SIZE, MysqlSource.MysqlSourceConfig.DEFAULT_FETCH_SIZE);
    properties.put(MysqlSource.MysqlSourceConfig.DATABASE, path.getDatabase());
    properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
    properties.put(MysqlSink.MysqlSinkConfig.TABLE_NAME, table);
  }

  @Override
  protected String getTableName(String database, String schema, String table) {
    return String.format("`%s`.`%s`", database, table);
  }

  @Override
  protected String getRandomQuery(String tableName, int limit) {
    // This query doesn't guarantee exactly "limit" number of rows
    // Note that we input "limit" with a trailing zero so that division gives an exact result
    return String.format("SELECT * FROM %s\n" +
                           "WHERE rand() < %d.0 / (SELECT COUNT(*) FROM %s)",
                         tableName, limit, tableName);
  }

  @Override
  protected String getStratifiedQuery(String tableName, int limit, String strata, String sessionID) {
    return String.format("WITH t_%s AS (\n" +
        "    SELECT *,\n" +
        "    ROW_NUMBER() OVER (ORDER BY %s, RAND()) AS sqn_%s,\n" +
        "    COUNT(*) OVER () AS c_%s\n" +
        "    FROM %s\n" +
        "  )\n" +
        "SELECT * FROM t_%s\n" +
        "WHERE MOD(sqn_%s, GREATEST(1, CAST(c_%s / %d AS UNSIGNED))) = 1\n" +
        "ORDER BY %s\n" +
        "LIMIT %d",
      sessionID, strata, sessionID, sessionID, tableName, sessionID, sessionID, sessionID,
      limit, strata, limit);
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, DBRecord dbRecord) {
    return dbRecord.getRecord();
  }
}

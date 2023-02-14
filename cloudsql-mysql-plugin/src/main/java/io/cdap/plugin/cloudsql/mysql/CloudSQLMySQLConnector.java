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

package io.cdap.plugin.cloudsql.mysql;

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
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import io.cdap.plugin.mysql.MysqlDBRecord;
import io.cdap.plugin.mysql.MysqlSchemaReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  A CLoudSQL MySQL Server Database Connector that connects to CloudSQL MySQL Server database via JDBC
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(CloudSQLMySQLConnector.NAME)
@Description("Connection to access data in CloudSQL MySQL Server databases using JDBC.")
@Category("Database")
public class CloudSQLMySQLConnector extends AbstractDBSpecificConnector<MysqlDBRecord> {
  
  public static final String NAME = CloudSQLMySQLConstants.PLUGIN_NAME;
  private final CloudSQLMySQLConnectorConfig config;

  public CloudSQLMySQLConnector(CloudSQLMySQLConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public boolean supportSchema() {
    return false;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return MysqlDBRecord.class;
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, MysqlDBRecord mysqlDBRecord) {
    return mysqlDBRecord.getRecord();
  }

  @Override
  protected List<Schema.Field> getSchemaFields(ResultSet resultSet, String sessionID) throws SQLException {
    return new MysqlSchemaReader(sessionID).getSchemaFields(resultSet);
  }

  @Override
  protected String getTableName(String database, String schema, String table) {
    return String.format("`%s`.`%s`", database, table);
  }

  @Override
  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> properties = new HashMap<>();
    setConnectionProperties(properties, request);
    builder
      .addRelatedPlugin(new PluginSpec(CloudSQLMySQLConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, properties))
      .addRelatedPlugin(new PluginSpec(CloudSQLMySQLConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, properties));

    String table = path.getTable();
    if (table == null) {
      return;
    }

    properties.put(CloudSQLMySQLSource.CloudSQLMySQLSourceConfig.IMPORT_QUERY, getTableQuery(path.getDatabase(),
                                                                                             path.getSchema(),
                                                                                             path.getTable()));
    properties.put(CloudSQLMySQLSource.CloudSQLMySQLSourceConfig.NUM_SPLITS, "1");
    properties.put(CloudSQLMySQLSource.CloudSQLMySQLSourceConfig.FETCH_SIZE,
                   CloudSQLMySQLSource.CloudSQLMySQLSourceConfig.DEFAULT_FETCH_SIZE);
    properties.put(ConnectionConfig.DATABASE, path.getDatabase());
    properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
    properties.put(CloudSQLMySQLSink.CloudSQLMySQLSinkConfig.TABLE_NAME, table);
  }
}

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
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * A SQL Server Database Connector that connects to SQL Server database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(SqlServerConnector.NAME)
@Description("Connection to browse and sample data from SQL Server databases using JDBC.")
@Category("Database")
public class SqlServerConnector extends AbstractDBSpecificConnector<SqlServerSourceDBRecord> {
  public static final String NAME = "SqlServer";
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
    Map<String, String> properties = new HashMap<>();
    setConnectionProperties(properties);
    builder.addRelatedPlugin(new PluginSpec(SqlServerConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, properties));
    String table = path.getTable();
    if (table == null) {
      return;
    }

    properties.put(SqlServerSource.SqlServerSourceConfig.IMPORT_QUERY, getTableQuery(path));
    properties.put(SqlServerSource.SqlServerSourceConfig.NUM_SPLITS, "1");
    properties.put(SqlServerSource.SqlServerSourceConfig.DATABASE, path.getDatabase());
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSourceSchemaReader();
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, SqlServerSourceDBRecord record) {
    return record.getRecord();
  }
}

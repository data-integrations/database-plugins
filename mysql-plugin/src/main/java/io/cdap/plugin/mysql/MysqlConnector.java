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

package io.cdap.plugin.mysql;

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
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * A Mysql Database Connector that connects to Mysql database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(MysqlConnector.NAME)
@Description("This connector creates connections to a MySQL database.")
@Category("Database")
public class MysqlConnector extends AbstractDBSpecificConnector<DBRecord> {
  public static final String NAME = "Mysql";
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
  protected Class<? extends DBWritable> getDBRecordType() {
    return DBRecord.class;
  }

  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> properties = new HashMap<>();
    setConnectionProperties(properties);
    builder.addRelatedPlugin(new PluginSpec(MysqlConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, properties));

    String table = path.getTable();
    if (table == null) {
      return;
    }

    properties.put(MysqlSource.MysqlSourceConfig.IMPORT_QUERY, getTableQuery(path));
    properties.put(MysqlSource.MysqlSourceConfig.NUM_SPLITS, "1");
    properties.put(MysqlSource.MysqlSourceConfig.DATABASE, path.getDatabase());
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, DBRecord record) {
    return record.getRecord();
  }
}

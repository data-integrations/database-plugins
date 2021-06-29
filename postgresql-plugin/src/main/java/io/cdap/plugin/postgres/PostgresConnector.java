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
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A Postgre SQL Database Connector that connects to Postgre SQL database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(PostgresConnector.NAME)
@Description("This connector creates connections to a Postgre SQL database.")
@Category("Database")
public class PostgresConnector extends AbstractDBSpecificConnector<PostgresDBRecord> {
  public static final String NAME = "Postgres";

  protected PostgresConnector(AbstractDBSpecificConnectorConfig config) {
    super(config);
  }

  @Override
  public boolean supportSchema() {
    return true;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return PostgresDBRecord.class;
  }

  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> properties = new HashMap<>();
    properties.put(PostgresSource.PostgresSourceConfig.NAME_USE_CONNECTION, "true");
    properties.put(PostgresSource.PostgresSourceConfig.NAME_CONNECTION, request.getConnectionWithMacro());
    builder.addRelatedPlugin(new PluginSpec(PostgresConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, properties));

    String table = path.getTable();
    if (table == null) {
      return;
    }

    properties.put(PostgresSource.PostgresSourceConfig.IMPORT_QUERY,
                   String.format("SELECT * FROM %s.%s.%s;", path.getDatabase(), path.getSchema(), table));
    properties.put(PostgresSource.PostgresSourceConfig.NUM_SPLITS, "1");
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new PostgresSchemaReader();
  }

  @Override
  public StructuredRecord transform(LongWritable longWritable, PostgresDBRecord record) {
    return record.getRecord();
  }

  protected ResultSet queryDatabases(Connection connection) throws SQLException {
    return connection.createStatement().executeQuery(String.format(
      "SELECT datname AS %s FROM pg_database WHERE datistemplate = false;",
      RESULTSET_COLUMN_TABLE_CAT));
  }

  @Override
  protected String getConnectionString(@Nullable String database) {
    if (database == null) {
      return config.getConnectionString();
    }
    return String
      .format(PostgresConstants.POSTGRES_CONNECTION_STRING_WITH_DB_FORMAT, config.getHost(), config.getPort(), database);
  }
}

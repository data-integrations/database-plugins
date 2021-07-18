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
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A Oracle Database Connector that connects to Oracle database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(OracleConnector.NAME)
@Description("Connection to browse and sample data from Oracle databases using JDBC.")
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
    Map<String, String> properties = new HashMap<>();
    setConnectionProperties(properties);
    builder.addRelatedPlugin(new PluginSpec(OracleConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, properties));

    String table = path.getTable();
    if (table == null) {
      return;
    }

    properties.put(OracleSource.OracleSourceConfig.IMPORT_QUERY, getTableQuery(path));
    properties.put(OracleSource.OracleSourceConfig.NUM_SPLITS, "1");
    properties.put(OracleSource.OracleSourceConfig.DATABASE, path.getDatabase());
    properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(table));
  }

  @Override
  protected void setConnectionProperties(Map<String, String> properties) {
    super.setConnectionProperties(properties);
    properties.put(OracleConstants.ROLE, config.getRole());
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSourceSchemaReader();
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
    if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(config)) {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SERVICE_NAME_WITH_DB_FORMAT, config.getHost(),
                           config.getPort(), database);
    }
    return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SID_WITH_DB_FORMAT,
                         config.getHost(), config.getPort(), database);
  }

  @Override
  protected ResultSet queryDatabases(Connection connection) throws SQLException {
    return connection.createStatement()
      .executeQuery(String.format("SELECT NAME AS %s FROM V$DATABASE", RESULTSET_COLUMN_TABLE_CAT));
  }

  @Override
  protected String getTableQuery(DBConnectorPath path) {
    return String.format("SELECT * from %s.%s", path.getSchema(), path.getTable());
  }
}

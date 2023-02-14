/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.db.DBRecord;
import io.cdap.plugin.common.db.schemareader.SqlServerSourceSchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from MSSQL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SqlServerConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = SqlServerConnector.NAME)})
public class SqlServerSource extends AbstractDBSource<SqlServerSource.SqlServerSourceConfig> {

  private final SqlServerSourceConfig sqlServerSourceConfig;

  public SqlServerSource(SqlServerSourceConfig sqlServerSourceConfig) {
    super(sqlServerSourceConfig);
    this.sqlServerSourceConfig = sqlServerSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return sqlServerSourceConfig.getConnectionString();
  }

  @Override
  protected List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    return new SqlServerSourceSchemaReader().getSchemaFields(resultSet, null, null);
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return DBRecord.class;
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    String fqn = DBUtils.constructFQN("mssql",
                                      sqlServerSourceConfig.getConnection().getHost(),
                                      sqlServerSourceConfig.getConnection().getPort(),
                                      sqlServerSourceConfig.database, sqlServerSourceConfig.getReferenceName());
    Asset asset = Asset.builder(sqlServerSourceConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }

  /**
   * MSSQL source config.
   */
  public static class SqlServerSourceConfig extends AbstractDBSpecificSourceConfig {

    public static final String NAME_USE_CONNECTION = "useConnection";
    public static final String NAME_CONNECTION = "connection";

    @Name(NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private SqlServerConnectorConfig connection;

    @Name(DATABASE)
    @Description("Database name to connect to")
    @Macro
    public String database;

    @Name(SqlServerConstants.INSTANCE_NAME)
    @Description(SqlServerConstants.INSTANCE_NAME_DESCRIPTION)
    @Nullable
    public String instanceName;

    @Name(SqlServerConstants.QUERY_TIMEOUT)
    @Description(SqlServerConstants.QUERY_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer queryTimeout = -1;

    @Name(SqlServerConstants.CONNECT_TIMEOUT)
    @Description(SqlServerConstants.CONNECT_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer connectTimeout;

    @Name(SqlServerConstants.COLUMN_ENCRYPTION)
    @Description(SqlServerConstants.COLUMN_ENCRYPTION_DESCRIPTION)
    @Nullable
    public Boolean columnEncryption;

    @Name(SqlServerConstants.ENCRYPT)
    @Description(SqlServerConstants.ENCRYPT_DESCRIPTION)
    @Nullable
    public Boolean encrypt;

    @Name(SqlServerConstants.TRUST_SERVER_CERTIFICATE)
    @Description(SqlServerConstants.TRUST_SERVER_CERTIFICATE_DESCRIPTION)
    @Nullable
    public Boolean trustServerCertificate;

    @Name(SqlServerConstants.WORKSTATION_ID)
    @Description(SqlServerConstants.WORKSTATION_ID_DESCRIPTION)
    @Nullable
    public String workstationId;

    @Name(SqlServerConstants.FAILOVER_PARTNER)
    @Description(SqlServerConstants.FAILOVER_PARTNER_DESCRIPTION)
    @Nullable
    public String failoverPartner;

    @Name(SqlServerConstants.PACKET_SIZE)
    @Description(SqlServerConstants.PACKET_SIZE_DESCRIPTION)
    @Nullable
    public Integer packetSize;

    @Name(SqlServerConstants.CURRENT_LANGUAGE)
    @Description(SqlServerConstants.CURRENT_LANGUAGE_DESCRIPTION)
    @Nullable
    public String currentLanguage;

    @Override
    public String getConnectionString() {
      return String
        .format(SqlServerConstants.SQL_SERVER_CONNECTION_STRING_FORMAT, connection.getHost(), connection.getPort(),
                database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return SqlServerUtil.composeDbSpecificArgumentsMap(instanceName, connection.getAuthenticationType(), null,
                                                         connectTimeout, columnEncryption, encrypt,
                                                         trustServerCertificate, workstationId, failoverPartner,
                                                         packetSize, queryTimeout);
    }

    @Override
    protected AbstractDBSpecificConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public List<String> getInitQueries() {
      if (!Strings.isNullOrEmpty(currentLanguage)) {
        return Collections.singletonList(String.format(SqlServerConstants.SET_LANGUAGE_QUERY_FORMAT, currentLanguage));
      }

      return Collections.emptyList();
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }

    @Override
    protected void validateField(FailureCollector collector, Schema.Field field, Schema actualFieldSchema,
                                 Schema expectedFieldSchema) {
      // we allow the case when actual type is Datetime but user manually set it to timestamp (datetime and datetime2)
      // or string (datetimeoffset). To make it compatible with old behavior that convert datetime to timestamp.
      // below validation is kind of loose, it's possible users try to manually map datetime to string or
      // map datetimeoffset to timestamp which is invalid. In such case runtime will still fail even validation passes.
      // But we don't have the original source type information here and don't want to do big refactoring here
      if (actualFieldSchema.getLogicalType() == Schema.LogicalType.DATETIME &&
            expectedFieldSchema.getLogicalType() == Schema.LogicalType.TIMESTAMP_MICROS ||
            actualFieldSchema.getLogicalType() == Schema.LogicalType.DATETIME &&
              expectedFieldSchema.getType() == Schema.Type.STRING) {
        return;
      }
      super.validateField(collector, field, actualFieldSchema, expectedFieldSchema);
    }

    @Override
    public boolean canConnect() {
      return super.canConnect() && !containsMacro(DATABASE);
    }
  }
}

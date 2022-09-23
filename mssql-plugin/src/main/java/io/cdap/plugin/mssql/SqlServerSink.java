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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import io.cdap.plugin.db.batch.sink.FieldsValidator;
import io.cdap.plugin.util.DBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink support for a MSSQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SqlServerConstants.PLUGIN_NAME)
@Description("Writes records to a MSSQL table. Each record will be written in a row in the table")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = SqlServerConnector.NAME)})
public class SqlServerSink extends AbstractDBSink<SqlServerSink.SqlServerSinkConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerSink.class);

  private final SqlServerSinkConfig sqlServerSinkConfig;

  public SqlServerSink(SqlServerSinkConfig sqlServerSinkConfig) {
    super(sqlServerSinkConfig);
    this.sqlServerSinkConfig = sqlServerSinkConfig;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSinkSchemaReader();
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new SqlServerSinkDBRecord(output, columnTypes);
  }

  @Override
  protected FieldsValidator getFieldsValidator() {
    return new SqlFieldsValidator();
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    String fqn = DBUtils.constructFQN("mssql",
                                      sqlServerSinkConfig.getConnection().getHost(),
                                      sqlServerSinkConfig.getConnection().getPort(),
                                      sqlServerSinkConfig.database, sqlServerSinkConfig.getReferenceName());
    Asset asset = Asset.builder(sqlServerSinkConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }

  /**
   * MSSQL action configuration.
   */
  public static class SqlServerSinkConfig extends AbstractDBSpecificSinkConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private SqlServerConnectorConfig connection;

    @Name(ConnectionConfig.DATABASE)
    @Description("Database name to connect to")
    @Macro
    private String database;

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
    public Map<String, String> getDBSpecificArguments() {
      return SqlServerUtil.composeDbSpecificArgumentsMap(instanceName, connection.getAuthenticationType(), null,
                                                         connectTimeout, columnEncryption, encrypt,
                                                         trustServerCertificate, workstationId, failoverPartner,
                                                         packetSize, queryTimeout);
    }

    @Override
    public String getConnectionString() {
      return String.format(SqlServerConstants.SQL_SERVER_CONNECTION_STRING_FORMAT,
                           connection.getHost(), connection.getPort(), database);
    }

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
    }

    @Override
    protected SqlServerConnectorConfig getConnection() {
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
    public boolean canConnect() {
      return super.canConnect() && !containsMacro(ConnectionConfig.DATABASE);
    }
  }
}

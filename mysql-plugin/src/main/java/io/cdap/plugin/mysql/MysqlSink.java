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

package io.cdap.plugin.mysql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.AbstractDBSpecificSinkConfig;
import io.cdap.plugin.db.sink.AbstractDBSink;
import io.cdap.plugin.db.sink.FieldsValidator;
import io.cdap.plugin.util.DBUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Sink support for a MySQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MysqlConstants.PLUGIN_NAME)
@Description("Writes records to a MySQL table. Each record will be written in a row in the table")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = MysqlConnector.NAME)})
public class MysqlSink extends AbstractDBSink<MysqlSink.MysqlSinkConfig> {

  private final MysqlSinkConfig mysqlSinkConfig;
  private static final Character ESCAPE_CHAR = '`';

  public MysqlSink(MysqlSinkConfig mysqlSinkConfig) {
    super(mysqlSinkConfig);
    this.mysqlSinkConfig = mysqlSinkConfig;
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new MysqlDBRecord(output, columnTypes);
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    String fqn = DBUtils.constructFQN("mysql",
                                      mysqlSinkConfig.getConnection().getHost(),
                                      mysqlSinkConfig.getConnection().getPort(),
                                      mysqlSinkConfig.database, mysqlSinkConfig.getReferenceName());
    Asset asset = Asset.builder(mysqlSinkConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }

  @Override
  protected FieldsValidator getFieldsValidator() {
    return new MysqlFieldsValidator();
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new MysqlSchemaReader(null);
  }

  @Override
  protected void setColumnsInfo(List<Schema.Field> fields) {
    List<String> columnsList = new ArrayList<>();
    StringJoiner columnsJoiner = new StringJoiner(",");
    for (Schema.Field field : fields) {
      columnsList.add(field.getName());
      columnsJoiner.add(ESCAPE_CHAR + field.getName() + ESCAPE_CHAR);
    }

    super.columns = Collections.unmodifiableList(columnsList);
    super.dbColumns = columnsJoiner.toString();
  }

  @VisibleForTesting
  String getDbColumns() {
    return dbColumns;
  }

  /**
   * MySQL action configuration.
   */
  public static class MysqlSinkConfig extends AbstractDBSpecificSinkConfig {
    
    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private MysqlConnectorConfig connection;

    @Name(ConnectionConfig.DATABASE)
    @Description("Database name to connect to")
    @Macro
    private String database;

    @Name(MysqlConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MysqlConstants.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Name(MysqlConstants.SQL_MODE)
    @Description("Override the default SQL_MODE session variable used by the server")
    @Nullable
    public String sqlMode;

    @Name(MysqlConstants.USE_SSL)
    @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
    @Nullable
    public String useSSL;

    @Name(MysqlConstants.CLIENT_CERT_KEYSTORE_URL)
    @Description("URL to the client certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String clientCertificateKeyStoreUrl;

    @Name(MysqlConstants.CLIENT_CERT_KEYSTORE_PASSWORD)
    @Description("Password for the client certificates KeyStore")
    @Nullable
    public String clientCertificateKeyStorePassword;

    @Name(MysqlConstants.TRUST_CERT_KEYSTORE_URL)
    @Description("URL to the trusted root certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String trustCertificateKeyStoreUrl;

    @Name(MysqlConstants.TRUST_CERT_KEYSTORE_PASSWORD)
    @Description("Password for the trusted root certificates KeyStore")
    @Nullable
    public String trustCertificateKeyStorePassword;

    @Override
    public String getConnectionString() {
      return MysqlUtil.getConnectionString(connection.getHost(), connection.getPort(), database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return MysqlUtil.composeDbSpecificArgumentsMap(autoReconnect, useCompression, useSSL,
                                                     clientCertificateKeyStoreUrl,
                                                     clientCertificateKeyStorePassword,
                                                     trustCertificateKeyStoreUrl,
                                                     trustCertificateKeyStorePassword, false);
    }

    @Override
    public MysqlConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
    }

    @Override
    public List<String> getInitQueries() {
      if (!Strings.isNullOrEmpty(sqlMode)) {
        return Collections.singletonList(String.format(MysqlConstants.SET_SQL_MODE_QUERY_FORMAT, sqlMode));
      }
      return Collections.emptyList();
    }

    @Override
    public boolean canConnect() {
      return super.canConnect() && !containsMacro(ConnectionConfig.DATABASE);
    }
  }
}

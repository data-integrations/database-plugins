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
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.source.AbstractDBSource;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from MySQL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MysqlConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = MysqlConnector.NAME)})
public class MysqlSource extends AbstractDBSource<MysqlSource.MysqlSourceConfig> {

  private final MysqlSourceConfig mysqlSourceConfig;

  public MysqlSource(MysqlSourceConfig mysqlSourceConfig) {
    super(mysqlSourceConfig);
    this.mysqlSourceConfig = mysqlSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return mysqlSourceConfig.getConnectionString();
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return MysqlDBRecord.class;
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    String fqn = DBUtils.constructFQN("mysql",
                                      mysqlSourceConfig.getConnection().getHost(),
                                      mysqlSourceConfig.getConnection().getPort(),
                                      mysqlSourceConfig.database, mysqlSourceConfig.getReferenceName());
    Asset asset = Asset.builder(mysqlSourceConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new MysqlSchemaReader(null);
  }

  /**
   * MySQL source config.
   */
  public static class MysqlSourceConfig extends AbstractDBSpecificSourceConfig {

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
    private MysqlConnectorConfig connection;

    @Name(DATABASE)
    @Description("Database name to connect to")
    @Macro
    public String database;

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

    @Name(MysqlConstants.USE_ANSI_QUOTES)
    @Description("Treats \" as an identifier quote character and not as a string quote character")
    @Nullable
    public Boolean useAnsiQuotes;

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
      // If connected to MySQL > 5.0.2, and setFetchSize() > 0 on a statement,
      // statement will use cursor-based fetching to retrieve rows
      boolean useCursorFetch = getFetchSize() != null && getFetchSize() > 0;
      return MysqlUtil.composeDbSpecificArgumentsMap(autoReconnect, useCompression, useSSL,
                                                     clientCertificateKeyStoreUrl,
                                                     clientCertificateKeyStorePassword,
                                                     trustCertificateKeyStoreUrl,
                                                     trustCertificateKeyStorePassword, useCursorFetch);
    }

    @Override
    public List<String> getInitQueries() {
      List<String> initQueries = new ArrayList<>();
      if (useAnsiQuotes != null && useAnsiQuotes) {
        initQueries.add(MysqlConstants.ANSI_QUOTES_QUERY);
      }
      if (!Strings.isNullOrEmpty(sqlMode)) {
        initQueries.add(String.format(MysqlConstants.SET_SQL_MODE_QUERY_FORMAT, sqlMode));
      }
      return initQueries;
    }

    @Override
    public MysqlConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }

    @Override
    protected void validateField(FailureCollector collector,
                                 Schema.Field field,
                                 Schema actualFieldSchema,
                                 Schema expectedFieldSchema) {
      // Backward compatibility changes to support MySQL YEAR to Date type conversion
      if (Schema.LogicalType.DATE.equals(expectedFieldSchema.getLogicalType())
            && Schema.Type.INT.equals(actualFieldSchema.getType())) {
        return;
      }

      // Backward compatibility change to support MySQL MEDIUMINT UNSIGNED to Long type conversion
      if (Schema.Type.LONG.equals(expectedFieldSchema.getType())
            && Schema.Type.INT.equals(actualFieldSchema.getType())) {
        return;
      }

      // Backward compatibility change to support MySQL TINYINT(1) to Bool type conversion
      if (Schema.Type.BOOLEAN.equals(expectedFieldSchema.getType())
              && Schema.Type.INT.equals(actualFieldSchema.getType())) {
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

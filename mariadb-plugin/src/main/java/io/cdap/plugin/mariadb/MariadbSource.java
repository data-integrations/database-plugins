/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.mariadb;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.source.AbstractDBSource;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * MariaDB source.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MariadbConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class MariadbSource extends AbstractDBSource<MariadbSource.MariadbSourceConfig> {

  private final MariadbSourceConfig mariadbSourceConfig;

  /**
   * This is the constructor for MariadbSource.
   * @param mariadbSourceConfig It takes Mariadb source config object as the parameter.
   */
  public MariadbSource(MariadbSourceConfig mariadbSourceConfig) {
    super(mariadbSourceConfig);
    this.mariadbSourceConfig = mariadbSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return String.format(MariadbConstants.MARIADB_CONNECTION_STRING_FORMAT,
                         mariadbSourceConfig.host, mariadbSourceConfig.port, mariadbSourceConfig.database);
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return MariadbDBRecord.class;
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    String fqn = DBUtils.constructFQN("mariadb",
      mariadbSourceConfig.host,
      mariadbSourceConfig.port,
      mariadbSourceConfig.database,
      mariadbSourceConfig.getReferenceName());
    Asset asset = Asset.builder(mariadbSourceConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new MariadbSchemaReader(null, mariadbSourceConfig.getConnectionArguments());
  }

  @Override
  protected String getErrorDetailsProviderClassName() {
    return MariadbErrorDetailsProvider.class.getName();
  }

  @Override
  protected String getExternalDocumentationLink() {
    return DBUtils.MARIADB_SUPPORTED_DOC_URL;
  }

  /**
   * MaraiDB source mariadbSourceConfig.
   */
  public static class MariadbSourceConfig extends DBSpecificSourceConfig {
    private static final String JDBC_PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
    private static final String JDBC_PROPERTY_SOCKET_TIMEOUT = "socketTimeout";
    private static final String JDBC_REWRITE_BATCHED_STATEMENTS = "rewriteBatchedStatements";

    private static final String MARIADB_TINYINT1_IS_BIT = "tinyInt1isBit";

    @Name(MariadbConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MariadbConstants.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Name(MariadbConstants.USE_SSL)
    @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
    @Nullable
    public String useSSL;

    @Name(MariadbConstants.USE_ANSI_QUOTES)
    @Description("Treats \" as an identifier quote character and not as a string quote character")
    @Nullable
    public Boolean useAnsiQuotes;

    @Name(MariadbConstants.KEY_STORE)
    @Description("URL to the client certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String keyStore;

    @Name(MariadbConstants.KEY_STORE_PASSWORD)
    @Description("Password for the client certificates KeyStore")
    @Nullable
    public String keyStorePassword;

    @Name(MariadbConstants.TRUST_STORE)
    @Description("URL to the trusted root certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String trustStore;

    @Name(MariadbConstants.TRUST_STORE_PASSWORD)
    @Description("Password for the trusted root certificates KeyStore")
    @Nullable
    public String trustStorePassword;

    @Override
    public String getConnectionString() {
      return MariadbUtil.getConnectionString(host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return MariadbUtil.composeDbSpecificArgumentsMap(autoReconnect, useCompression, useSSL,
              keyStore,
              keyStorePassword,
              trustStore,
              trustStorePassword);
    }

    @Override
    public List<String> getInitQueries() {
      return MariadbUtil.composeDbInitQueries(useAnsiQuotes);
    }

    @Override
    public Map<String, String> getConnectionArguments() {
      Map<String, String> arguments = new HashMap<>(super.getConnectionArguments());
      // the unit below is millisecond
      arguments.putIfAbsent(JDBC_PROPERTY_CONNECT_TIMEOUT, "20000");
      arguments.putIfAbsent(JDBC_PROPERTY_SOCKET_TIMEOUT, "20000");
      arguments.putIfAbsent(JDBC_REWRITE_BATCHED_STATEMENTS, "true");
      // MariaDB property to ensure that TINYINT(1) type data is not converted to MariaDB Bit/Boolean type in the
      // ResultSet.
      arguments.putIfAbsent(MARIADB_TINYINT1_IS_BIT, "false");
      return arguments;
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
  }
}

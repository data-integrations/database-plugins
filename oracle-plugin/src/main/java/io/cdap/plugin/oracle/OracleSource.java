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

package io.cdap.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from Oracle.
 */
@Plugin(type = "batchsource")
@Name(OracleConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = OracleConnector.NAME)})
public class OracleSource extends AbstractDBSource<OracleSource.OracleSourceConfig> {

  private final OracleSourceConfig oracleSourceConfig;

  public OracleSource(OracleSourceConfig oracleSourceConfig) {
    super(oracleSourceConfig);
    this.oracleSourceConfig = oracleSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return oracleSourceConfig.getConnectionString();
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSourceSchemaReader();
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return OracleSourceDBRecord.class;
  }

  /**
   * Oracle source config.
   */
  public static class OracleSourceConfig extends AbstractDBSpecificSourceConfig {

    public static final String NAME_USE_CONNECTION = "useConnection";
    public static final String NAME_CONNECTION = "connection";
    public static final String DEFAULT_ROW_PREFETCH_VALUE = "40";
    public static final String DEFAULT_BATCH_SIZE = "10";

    @Name(NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private OracleConnectorConfig connection;

    @Name(OracleConstants.DEFAULT_BATCH_VALUE)
    @Description("The default batch value that triggers an execution request.")
    @Nullable
    private Integer defaultBatchValue;

    @Name(OracleConstants.DEFAULT_ROW_PREFETCH)
    @Description("The default number of rows to prefetch from the server.")
    @Nullable
    private Integer defaultRowPrefetch;

    @Override
    public String getConnectionString() {
      if (OracleConstants.TNS_CONNECTION_TYPE.equals(connection.getConnectionType())) {
        return String.format(OracleConstants.ORACLE_CONNECTION_STRING_TNS_FORMAT, connection.getDatabase());
      } else if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(connection.getConnectionType())) {
        return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SERVICE_NAME_FORMAT, connection.getHost(),
                connection.getPort(), connection.getDatabase());
      } else {
        return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SID_FORMAT,
                connection.getHost(), connection.getPort(), connection.getDatabase());
      }
    }

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      if (defaultBatchValue != null) {
        builder.put(OracleConstants.DEFAULT_BATCH_VALUE, String.valueOf(defaultBatchValue));
      }
      if (defaultRowPrefetch != null) {
        builder.put(OracleConstants.DEFAULT_ROW_PREFETCH, String.valueOf(defaultRowPrefetch));
      }

      return builder.build();
    }

    @Override
    protected OracleConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }

    @Override
    public String getTransactionIsolationLevel() {
      return connection.getTransactionIsolationLevel();
    }
  }

  /**
   * Oracle specific schema request.
   */
  private static class OracleSchemaRequest extends GetSchemaRequest {
    @Nullable
    public String connectionType;
  }
}

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

package io.cdap.plugin.postgres;

import com.google.common.collect.ImmutableMap;
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
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from PostgreSQL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = PostgresConnector.NAME)})
public class PostgresSource extends AbstractDBSource<PostgresSource.PostgresSourceConfig> {

  private final PostgresSourceConfig postgresSourceConfig;

  public PostgresSource(PostgresSourceConfig postgresSourceConfig) {
    super(postgresSourceConfig);
    this.postgresSourceConfig = postgresSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return postgresSourceConfig.getConnectionString();
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new PostgresSchemaReader();
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return PostgresDBRecord.class;
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    String fqn = DBUtils.constructFQN("postgres",
                                      postgresSourceConfig.getConnection().getHost(),
                                      postgresSourceConfig.getConnection().getPort(),
                                      postgresSourceConfig.getConnection().getDatabase(),
                                      postgresSourceConfig.getReferenceName());
    Asset asset = Asset.builder(postgresSourceConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }

  /**
   * PosgtreSQL source config.
   */
  public static class PostgresSourceConfig extends AbstractDBSpecificSourceConfig {

    public static final String NAME_USE_CONNECTION = "useConnection";
    public static final String NAME_CONNECTION = "connection";
    public static final String DEFAULT_CONNECTION_TIMEOUT_SECONDS = "100";

    @Name(NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private PostgresConnectorConfig connection;

    @Name(PostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String
        .format(PostgresConstants.POSTGRES_CONNECTION_STRING_WITH_DB_FORMAT, connection.getHost(), connection.getPort(),
                connection.getDatabase());
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(PostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }

    @Override
    public Integer getFetchSize() {
      Integer fetchSize = super.getFetchSize();
      return fetchSize == null ? Integer.parseInt(DEFAULT_FETCH_SIZE) : fetchSize;
    }

    @Override
    protected PostgresConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }

    @Override
    protected void validateField(FailureCollector collector, Schema.Field field, Schema actualFieldSchema,
                                 Schema expectedFieldSchema) {

      // This change is needed to make sure that the pipeline upgrade continues to work post upgrade.
      // Since the older handling of the precision less used to convert to the decimal type,
      // and the new version would try to convert to the String type. In that case the output schema would
      // contain Decimal(38, 0) (or something similar), and the code internally would try to identify
      // the schema of the field(without precision and scale) as String.
      if (Schema.LogicalType.DECIMAL.equals(expectedFieldSchema.getLogicalType()) &&
        actualFieldSchema.getType().equals(Schema.Type.STRING)) {
        return;
      }
      super.validateField(collector, field, actualFieldSchema, expectedFieldSchema);
    }
  }
}


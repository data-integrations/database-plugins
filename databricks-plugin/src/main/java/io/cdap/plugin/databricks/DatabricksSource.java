/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.databricks;

import com.google.common.annotations.VisibleForTesting;
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
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.TransactionIsolationLevel;
import io.cdap.plugin.db.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.source.AbstractDBSource;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from a Databricks database.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(DatabricksConstants.PLUGIN_NAME)
@Description(
  "Reads from a Databricks table using a configurable SQL query."
    + " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = DatabricksConnector.NAME)})
public class DatabricksSource extends AbstractDBSource<DatabricksSource.DatabricksSourceConfig> {

  private final DatabricksSourceConfig databricksSourceConfig;

  public DatabricksSource(DatabricksSourceConfig databricksSourceConfig) {
    super(databricksSourceConfig);
    this.databricksSourceConfig = databricksSourceConfig;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new DatabricksSchemaReader();
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return DatabricksDBRecord.class;
  }

  @Override
  protected String createConnectionString() {
    DatabricksConnectorConfig connection = databricksSourceConfig.getConnection();
    return connection == null ? null : connection.getConnectionString();
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    DatabricksConnectorConfig connection = databricksSourceConfig.getConnection();
    String host = connection == null ? null : connection.getHost();
    int port = connection == null ? 443 : connection.getPort();
    String database = connection == null ? null : connection.getDatabase();
    String fqn = DBUtils.constructFQN("databricks", host, port, database,
                                      databricksSourceConfig.getReferenceName());
    Asset.Builder assetBuilder = Asset.builder(databricksSourceConfig.getReferenceName()).setFqn(fqn);
    return new LineageRecorder(context, assetBuilder.build());
  }

  @Override
  public ConnectionConfigAccessor getConnectionConfigAccessor(String driverClassName,
                                                               Schema schemaFromDB,
                                                               FailureCollector collector) throws IOException {
    ConnectionConfigAccessor configAccessor =
      super.getConnectionConfigAccessor(driverClassName, schemaFromDB, collector);
    configAccessor.setAutoCommitEnabled(true);
    return configAccessor;
  }

  /**
   * Databricks source config.
   */
  public static class DatabricksSourceConfig extends AbstractDBSpecificSourceConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private DatabricksConnectorConfig connection;

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return Collections.emptyMap();
    }

    @VisibleForTesting
    public DatabricksSourceConfig(@Nullable Boolean useConnection,
                                  @Nullable DatabricksConnectorConfig connection) {
      this.useConnection = useConnection;
      this.connection = connection;
    }

    @Override
    public String getTransactionIsolationLevel() {
      return TransactionIsolationLevel.Level.TRANSACTION_READ_UNCOMMITTED.name();
    }

    @Override
    public Integer getFetchSize() {
      Integer fetchSize = super.getFetchSize();
      return fetchSize == null ? Integer.parseInt(DEFAULT_FETCH_SIZE) : fetchSize;
    }

    @Override
    protected DatabricksConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }
  }
}

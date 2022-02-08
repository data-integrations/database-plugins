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

package io.cdap.plugin.cloudsql.mysql;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;

import java.util.Map;
import javax.annotation.Nullable;

/** Sink support for a CloudSQL MySQL database. */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(CloudSQLMySQLConstants.PLUGIN_NAME)
@Description(
    "Writes records to a CloudSQL MySQL table. Each record will be written in a row in the table.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = CloudSQLMySQLConnector.NAME)})
public class CloudSQLMySQLSink extends AbstractDBSink<CloudSQLMySQLSink.CloudSQLMySQLSinkConfig> {

  private final CloudSQLMySQLSinkConfig cloudsqlMysqlSinkConfig;

  public CloudSQLMySQLSink(CloudSQLMySQLSinkConfig cloudsqlMysqlSinkConfig) {
    super(cloudsqlMysqlSinkConfig);
    this.cloudsqlMysqlSinkConfig = cloudsqlMysqlSinkConfig;
  }
  
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    
    CloudSQLMySQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlMysqlSinkConfig.connection.getInstanceType(),
        cloudsqlMysqlSinkConfig.connection.getConnectionName());
    
    super.configurePipeline(pipelineConfigurer);
  }
  
  @Override
  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }
  
  /** CloudSQL MySQL sink configuration. */
  public static class CloudSQLMySQLSinkConfig extends AbstractDBSpecificSinkConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private CloudSQLMySQLConnectorConfig connection;

    @Name(CloudSQLMySQLConstants.CONNECTION_TIMEOUT)
    @Description(
        "The timeout value used for socket connect operations. If connecting to the server takes longer "
            + "than this value, the connection is broken. The timeout is specified in seconds and a value "
            + "of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Name(TRANSACTION_ISOLATION_LEVEL)
    @Description("Transaction isolation level for queries run by this sink.")
    @Nullable
    public String transactionIsolationLevel;

    @Override
    public String getTransactionIsolationLevel() {
      return transactionIsolationLevel;
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(
          CloudSQLMySQLConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }

    @Override
    @Nullable
    protected CloudSQLMySQLConnectorConfig getConnection() {
      return connection;
    }
  }
}

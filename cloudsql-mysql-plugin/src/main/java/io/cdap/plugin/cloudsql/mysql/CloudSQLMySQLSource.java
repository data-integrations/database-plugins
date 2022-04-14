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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Batch source to read from CloudSQL MySQL. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(CloudSQLMySQLConstants.PLUGIN_NAME)
@Description(
    "Reads from a CloudSQL database table using a configurable SQL query."
        + " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = CloudSQLMySQLConnector.NAME)})
public class CloudSQLMySQLSource extends AbstractDBSource<CloudSQLMySQLSource.CloudSQLMySQLSourceConfig> {

  private final CloudSQLMySQLSourceConfig cloudsqlMysqlSourceConfig;

  public CloudSQLMySQLSource(CloudSQLMySQLSourceConfig cloudsqlMysqlSourceConfig) {
    super(cloudsqlMysqlSourceConfig);
    this.cloudsqlMysqlSourceConfig = cloudsqlMysqlSourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();

    if (!cloudsqlMysqlSourceConfig.containsMacro(CloudSQLMySQLConstants.INSTANCE_TYPE) &
    !cloudsqlMysqlSourceConfig.containsMacro(CloudSQLMySQLConstants.CONNECTION_NAME)) {
      CloudSQLMySQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlMysqlSourceConfig.connection.getInstanceType(),
        cloudsqlMysqlSourceConfig.connection.getConnectionName());
    }

    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }

  @Override
  protected String createConnectionString() {
    if (CloudSQLMySQLConstants.PRIVATE_INSTANCE.equalsIgnoreCase(
        cloudsqlMysqlSourceConfig.connection.getInstanceType())) {
      return String.format(
          CloudSQLMySQLConstants.PRIVATE_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
          cloudsqlMysqlSourceConfig.connection.getConnectionName(),
          cloudsqlMysqlSourceConfig.connection.getDatabase());
    }

    return String.format(
        CloudSQLMySQLConstants.PUBLIC_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
        cloudsqlMysqlSourceConfig.connection.getDatabase(),
        cloudsqlMysqlSourceConfig.connection.getConnectionName());
  }

  /** CloudSQL MySQL source config. */
  public static class CloudSQLMySQLSourceConfig extends AbstractDBSpecificSourceConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private CloudSQLMySQLConnectorConfig connection;

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      if (getFetchSize() == null || getFetchSize() <= 0) {
        return Collections.emptyMap();
      }
      Map<String, String> arguments = new HashMap<>();
      // If connected to MySQL > 5.0.2, and setFetchSize() > 0 on a statement,
      // statement will use cursor-based fetching to retrieve rows
      arguments.put("useCursorFetch", "true");
      return arguments;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }

    @Override
    protected CloudSQLMySQLConnectorConfig getConnection() {
      return connection;
    }
  }
}

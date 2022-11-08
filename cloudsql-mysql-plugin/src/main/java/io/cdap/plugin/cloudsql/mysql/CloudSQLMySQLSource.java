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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.mysql.MysqlDBRecord;
import io.cdap.plugin.util.CloudSQLUtil;
import io.cdap.plugin.util.DBUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

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

    if (!cloudsqlMysqlSourceConfig.containsMacro(CloudSQLUtil.INSTANCE_TYPE) &&
    !cloudsqlMysqlSourceConfig.containsMacro(CloudSQLUtil.CONNECTION_NAME)) {
      CloudSQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlMysqlSourceConfig.connection.getInstanceType(),
        cloudsqlMysqlSourceConfig.connection.getConnectionName());
    }

    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return MysqlDBRecord.class;
  }

  @Override
  protected String createConnectionString() {
    if (CloudSQLUtil.PRIVATE_INSTANCE.equalsIgnoreCase(
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

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    String host;
    String location = "";
    if (CloudSQLUtil.PRIVATE_INSTANCE.equalsIgnoreCase(cloudsqlMysqlSourceConfig.getConnection().getInstanceType())) {
      // connection is the private IP address
      host = cloudsqlMysqlSourceConfig.getConnection().getConnectionName();
    } else {
      // connection is of the form <projectId>:<region>:<instanceName>
      String[] connectionParams = cloudsqlMysqlSourceConfig.getConnection().getConnectionName().split(":");
      host = connectionParams[2];
      location = connectionParams[1];
    }
    String fqn = DBUtils.constructFQN("mysql", host, 3306,
                                      cloudsqlMysqlSourceConfig.getConnection().getDatabase(),
                                      cloudsqlMysqlSourceConfig.getReferenceName());
    Asset.Builder assetBuilder = Asset.builder(cloudsqlMysqlSourceConfig.getReferenceName()).setFqn(fqn);
    if (!Strings.isNullOrEmpty(location)) {
      assetBuilder.setLocation(location);
    }
    return new LineageRecorder(context, assetBuilder.build());
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

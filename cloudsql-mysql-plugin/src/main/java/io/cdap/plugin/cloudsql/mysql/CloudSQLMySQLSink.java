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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.config.AbstractDBSpecificSinkConfig;
import io.cdap.plugin.db.sink.AbstractDBSink;
import io.cdap.plugin.mysql.MysqlDBRecord;
import io.cdap.plugin.util.CloudSQLUtil;
import io.cdap.plugin.util.DBUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/** Sink support for a CloudSQL MySQL database. */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(CloudSQLMySQLConstants.PLUGIN_NAME)
@Description(
    "Writes records to a CloudSQL MySQL table. Each record will be written in a row in the table.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = CloudSQLMySQLConnector.NAME)})
public class CloudSQLMySQLSink extends AbstractDBSink<CloudSQLMySQLSink.CloudSQLMySQLSinkConfig> {

  private final CloudSQLMySQLSinkConfig cloudsqlMysqlSinkConfig;
  private static final Character ESCAPE_CHAR = '`';

  public CloudSQLMySQLSink(CloudSQLMySQLSinkConfig cloudsqlMysqlSinkConfig) {
    super(cloudsqlMysqlSinkConfig);
    this.cloudsqlMysqlSinkConfig = cloudsqlMysqlSinkConfig;
  }
  
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();

    if (!cloudsqlMysqlSinkConfig.containsMacro(CloudSQLUtil.INSTANCE_TYPE) &&
      !cloudsqlMysqlSinkConfig.containsMacro(CloudSQLUtil.CONNECTION_NAME)) {
      CloudSQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlMysqlSinkConfig.connection.getInstanceType(),
        cloudsqlMysqlSinkConfig.connection.getConnectionName());
    }
    
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new MysqlDBRecord(output, columnTypes);
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

  @Override
  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    String host;
    String location = "";
    if (CloudSQLUtil.PRIVATE_INSTANCE.equalsIgnoreCase(cloudsqlMysqlSinkConfig.getConnection().getInstanceType())) {
      // connection is the private IP address
      host = cloudsqlMysqlSinkConfig.getConnection().getConnectionName();
    } else {
      // connection is of the form <projectId>:<region>:<instanceName>
      String[] connectionParams = cloudsqlMysqlSinkConfig.getConnection().getConnectionName().split(":");
      host = connectionParams[2];
      location = connectionParams[1];
    }
    String fqn = DBUtils.constructFQN("mysql", host,
                                      cloudsqlMysqlSinkConfig.getConnection().getPort(),
                                      cloudsqlMysqlSinkConfig.getConnection().getDatabase(),
                                      cloudsqlMysqlSinkConfig.getReferenceName());
    Asset.Builder assetBuilder = Asset.builder(cloudsqlMysqlSinkConfig.getReferenceName()).setFqn(fqn);
    if (!Strings.isNullOrEmpty(location)) {
      assetBuilder.setLocation(location);
    }
    return new LineageRecorder(context, assetBuilder.build());
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

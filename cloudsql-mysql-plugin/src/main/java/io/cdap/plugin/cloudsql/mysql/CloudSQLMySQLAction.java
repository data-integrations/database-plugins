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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.batch.action.AbstractDBAction;
import io.cdap.plugin.db.batch.action.QueryConfig;
import io.cdap.plugin.util.CloudSQLUtil;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Action that runs a MySQL command on a CloudSQL instance database.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(CloudSQLMySQLConstants.PLUGIN_NAME)
@Description("Action that runs a MySQL command on a CloudSQL instance database")
public class CloudSQLMySQLAction extends AbstractDBAction {

  private final CloudSQLMySQLActionConfig cloudsqlMysqlActionConfig;

  public CloudSQLMySQLAction(CloudSQLMySQLActionConfig cloudsqlMysqlActionConfig) {
    super(cloudsqlMysqlActionConfig, false);
    this.cloudsqlMysqlActionConfig = cloudsqlMysqlActionConfig;
  }
  
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    
    CloudSQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlMysqlActionConfig.instanceType,
        cloudsqlMysqlActionConfig.connectionName);
    
    super.configurePipeline(pipelineConfigurer);
  }

  /** Mysql Action Config. */
  public static class CloudSQLMySQLActionConfig extends QueryConfig {
    
    public CloudSQLMySQLActionConfig() {
      this.instanceType = CloudSQLUtil.PUBLIC_INSTANCE;
    }
  
    @Name(CloudSQLUtil.CONNECTION_NAME)
    @Macro
    @Description(
        "The CloudSQL instance to connect to. For a public instance, the connection string should be in the format "
            + "<PROJECT_ID>:<REGION>:<INSTANCE_NAME> which can be found in the instance overview page. For a private "
            + "instance, enter the internal IP address of the Compute Engine VM cloudsql proxy is running on.")
    public String connectionName;

    @Name(DATABASE)
    @Macro
    @Description("Database name to connect to")
    public String database;

    @Name(CloudSQLMySQLConstants.CONNECTION_TIMEOUT)
    @Description(
        "The timeout value used for socket connect operations. If connecting to the server takes longer "
            + "than this value, the connection is broken. The timeout is specified in seconds and a value "
            + "of zero means that it is disabled.")
    @Nullable
    public Integer connectionTimeout;

    @Name(CloudSQLUtil.INSTANCE_TYPE)
    @Description("Whether the CloudSQL instance to connect to is private or public.")
    @Nullable
    public String instanceType;

    @Override
    public String getConnectionString() {
      if (CloudSQLUtil.PRIVATE_INSTANCE.equalsIgnoreCase(instanceType)) {
        return String.format(
            CloudSQLMySQLConstants.PRIVATE_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
            connectionName,
            database);
      }
      
      return String.format(
          CloudSQLMySQLConstants.PUBLIC_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
          database,
          connectionName);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(
          CloudSQLMySQLConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }
  }
}

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

import com.google.common.net.InetAddresses;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.source.AbstractDBSource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/** Batch source to read from CloudSQL MySQL. */
@Plugin(type = "batchsource")
@Name(CloudSQLMySQLConstants.PLUGIN_NAME)
@Description(
    "Reads from a CloudSQL database table using a configurable SQL query."
        + " Outputs one record for each row returned by the query.")
public class CloudSQLMySQLSource extends AbstractDBSource {

  private final CloudSQLMySQLSourceConfig cloudsqlMysqlSourceConfig;

  public CloudSQLMySQLSource(CloudSQLMySQLSourceConfig cloudsqlMysqlSourceConfig) {
    super(cloudsqlMysqlSourceConfig);
    this.cloudsqlMysqlSourceConfig = cloudsqlMysqlSourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();

    CloudSQLMySQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlMysqlSourceConfig.instanceType,
        cloudsqlMysqlSourceConfig.connectionName);

    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }

  @Override
  protected String createConnectionString() {
    if (CloudSQLMySQLConstants.PRIVATE_INSTANCE.equalsIgnoreCase(
        cloudsqlMysqlSourceConfig.instanceType)) {
      return String.format(
          CloudSQLMySQLConstants.PRIVATE_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
          cloudsqlMysqlSourceConfig.connectionName,
          cloudsqlMysqlSourceConfig.database);
    }

    return String.format(
        CloudSQLMySQLConstants.PUBLIC_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
        cloudsqlMysqlSourceConfig.database,
        cloudsqlMysqlSourceConfig.connectionName);
  }

  /** CloudSQL MySQL source config. */
  public static class CloudSQLMySQLSourceConfig extends AbstractDBSource.DBSourceConfig {
  
    public CloudSQLMySQLSourceConfig() {
      this.instanceType = CloudSQLMySQLConstants.PUBLIC_INSTANCE;
    }

    @Name(CloudSQLMySQLConstants.CONNECTION_NAME)
    @Description(
        "The CloudSQL instance to connect to. For a public instance, the connection string should be in the format "
            + "<PROJECT_ID>:<REGION>:<INSTANCE_NAME> which can be found in the instance overview page. For a private "
            + "instance, enter the internal IP address of the Compute Engine VM cloudsql proxy is running on.")
    public String connectionName;

    @Name(DATABASE)
    @Description("Database name to connect to")
    public String database;

    @Name(CloudSQLMySQLConstants.INSTANCE_TYPE)
    @Description("Whether the CloudSQL instance to connect to is private or public.")
    @Nullable
    public String instanceType;

    @Override
    public String getConnectionString() {
      if (CloudSQLMySQLConstants.PRIVATE_INSTANCE.equalsIgnoreCase(instanceType)) {
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
  }
}

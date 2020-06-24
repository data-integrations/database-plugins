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

package io.cdap.plugin.cloudsql.postgres;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.postgres.PostgresDBRecord;
import io.cdap.plugin.postgres.PostgresSchemaReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import javax.annotation.Nullable;

/** Batch source to read from a CloudSQL PostgreSQL instance database. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(CloudSQLPostgreSQLConstants.PLUGIN_NAME)
@Description(
    "Reads from a CloudSQL PostgreSQL database table(s) using a configurable SQL query."
        + " Outputs one record for each row returned by the query.")
public class CloudSQLPostgreSQLSource extends AbstractDBSource {

  private final CloudSQLPostgreSQLSourceConfig cloudsqlPostgresqlSourceConfig;

  public CloudSQLPostgreSQLSource(CloudSQLPostgreSQLSourceConfig cloudsqlPostgresqlSourceConfig) {
    super(cloudsqlPostgresqlSourceConfig);
    this.cloudsqlPostgresqlSourceConfig = cloudsqlPostgresqlSourceConfig;
  }
  
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    
    CloudSQLPostgreSQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlPostgresqlSourceConfig.instanceType,
        cloudsqlPostgresqlSourceConfig.connectionName);
    
    super.configurePipeline(pipelineConfigurer);
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
  protected String createConnectionString() {
    if (CloudSQLPostgreSQLConstants.PRIVATE_INSTANCE.equalsIgnoreCase(
        cloudsqlPostgresqlSourceConfig.instanceType)) {
      return String.format(
          CloudSQLPostgreSQLConstants.PRIVATE_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
          cloudsqlPostgresqlSourceConfig.connectionName,
          cloudsqlPostgresqlSourceConfig.database);
    }

    return String.format(
        CloudSQLPostgreSQLConstants.PUBLIC_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
        cloudsqlPostgresqlSourceConfig.database,
        cloudsqlPostgresqlSourceConfig.connectionName);
  }

  /** CloudSQL PostgreSQL source config. */
  public static class CloudSQLPostgreSQLSourceConfig extends AbstractDBSource.DBSourceConfig {
    
    public CloudSQLPostgreSQLSourceConfig() {
      this.instanceType = CloudSQLPostgreSQLConstants.PUBLIC_INSTANCE;
    }
  
    @Name(CloudSQLPostgreSQLConstants.CONNECTION_NAME)
    @Description(
        "The CloudSQL instance to connect to. For a public instance, the connection string should be in the format "
            + "<PROJECT_ID>:<REGION>:<INSTANCE_NAME> which can be found in the instance overview page. For a private "
            + "instance, enter the internal IP address of the Compute Engine VM cloudsql proxy is running on.")
    public String connectionName;

    @Name(DATABASE)
    @Description("Database name to connect to")
    public String database;
  
    @Name(CloudSQLPostgreSQLConstants.INSTANCE_TYPE)
    @Description("Whether the CloudSQL instance to connect to is private or public.")
    @Nullable
    public String instanceType;
  
    @Override
    public String getConnectionString() {
      if (CloudSQLPostgreSQLConstants.PRIVATE_INSTANCE.equalsIgnoreCase(instanceType)) {
        return String.format(
            CloudSQLPostgreSQLConstants.PRIVATE_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
            connectionName,
            database);
      }
      
      return String.format(
          CloudSQLPostgreSQLConstants.PUBLIC_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
          database,
          connectionName);
    }
  }
}

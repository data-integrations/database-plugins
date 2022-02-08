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
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.postgres.PostgresDBRecord;
import io.cdap.plugin.postgres.PostgresSchemaReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/** Batch source to read from a CloudSQL PostgreSQL instance database. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(CloudSQLPostgreSQLConstants.PLUGIN_NAME)
@Description(
    "Reads from a CloudSQL PostgreSQL database table(s) using a configurable SQL query."
        + " Outputs one record for each row returned by the query.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = CloudSQLPostgreSQLConnector.NAME)})
public class CloudSQLPostgreSQLSource
  extends AbstractDBSource<CloudSQLPostgreSQLSource.CloudSQLPostgreSQLSourceConfig> {

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
        cloudsqlPostgresqlSourceConfig.connection.getInstanceType(),
        cloudsqlPostgresqlSourceConfig.connection.getConnectionName());
    
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
        cloudsqlPostgresqlSourceConfig.connection.getInstanceType())) {
      return String.format(
          CloudSQLPostgreSQLConstants.PRIVATE_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
          cloudsqlPostgresqlSourceConfig.connection.getConnectionName(),
          cloudsqlPostgresqlSourceConfig.connection.getDatabase());
    }

    return String.format(
        CloudSQLPostgreSQLConstants.PUBLIC_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
        cloudsqlPostgresqlSourceConfig.connection.getDatabase(),
        cloudsqlPostgresqlSourceConfig.connection.getConnectionName());
  }

  /** CloudSQL PostgreSQL source config. */
  public static class CloudSQLPostgreSQLSourceConfig extends AbstractDBSpecificSourceConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private CloudSQLPostgreSQLConnectorConfig connection;

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      return Collections.emptyMap();
    }

    @Override
    protected CloudSQLPostgreSQLConnectorConfig getConnection() {
      return connection;
    }

    @Override
    public void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      super.validate(collector);
    }
  }
}

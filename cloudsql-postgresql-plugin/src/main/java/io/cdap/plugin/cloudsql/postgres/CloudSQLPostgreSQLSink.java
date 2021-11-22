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
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.AbstractDBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import io.cdap.plugin.db.batch.sink.FieldsValidator;
import io.cdap.plugin.postgres.PostgresDBRecord;
import io.cdap.plugin.postgres.PostgresFieldsValidator;
import io.cdap.plugin.postgres.PostgresSchemaReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/** Sink support for a CloudSQL PostgreSQL database. */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(CloudSQLPostgreSQLConstants.PLUGIN_NAME)
@Description(
    "Writes records to a CloudSQL PostgreSQL table. Each record will be written in a row in the table")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = CloudSQLPostgreSQLConnector.NAME)})
public class CloudSQLPostgreSQLSink extends AbstractDBSink<CloudSQLPostgreSQLSink.CloudSQLPostgreSQLSinkConfig> {

  private static final Character ESCAPE_CHAR = '"';

  private final CloudSQLPostgreSQLSinkConfig cloudsqlPostgresqlSinkConfig;

  public CloudSQLPostgreSQLSink(CloudSQLPostgreSQLSinkConfig cloudsqlPostgresqlSinkConfig) {
    super(cloudsqlPostgresqlSinkConfig);
    this.cloudsqlPostgresqlSinkConfig = cloudsqlPostgresqlSinkConfig;
  }
  
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    
    CloudSQLPostgreSQLUtil.checkConnectionName(
        failureCollector,
        cloudsqlPostgresqlSinkConfig.connection.getInstanceType(),
        cloudsqlPostgresqlSinkConfig.connection.getConnectionName());
    
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new PostgresSchemaReader();
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new PostgresDBRecord(output, columnTypes);
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

  @Override
  protected FieldsValidator getFieldsValidator() {
    return new PostgresFieldsValidator();
  }

  /** CloudSQL PostgreSQL sink config. */
  public static class CloudSQLPostgreSQLSinkConfig extends AbstractDBSpecificSinkConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private CloudSQLPostgreSQLConnectorConfig connection;

    @Name(CloudSQLPostgreSQLConstants.CONNECTION_TIMEOUT)
    @Description(
        "The timeout value used for socket connect operations. If connecting to the server takes longer "
            + "than this value, the connection is broken. The timeout is specified in seconds and a value "
            + "of zero means that it is disabled")
    @Nullable
    private Integer connectionTimeout;

    @Name(TRANSACTION_ISOLATION_LEVEL)
    @Description("Transaction isolation level for queries run by this sink.")
    @Nullable
    private String transactionIsolationLevel;

    @Override
    public String getTransactionIsolationLevel() {
      return transactionIsolationLevel;
    }

    @Override
    public String getEscapedTableName() {
      return ESCAPE_CHAR + getTableName() + ESCAPE_CHAR;
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(
          CloudSQLPostgreSQLConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }

    @Override
    protected CloudSQLPostgreSQLConnectorConfig getConnection() {
      return connection;
    }
  }
}

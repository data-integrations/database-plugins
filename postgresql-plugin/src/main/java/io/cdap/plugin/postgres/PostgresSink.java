/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.postgres;

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
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.AbstractDBSpecificSinkConfig;
import io.cdap.plugin.db.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.sink.AbstractDBSink;
import io.cdap.plugin.db.sink.FieldsValidator;
import io.cdap.plugin.util.DBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * Sink support for a PostgreSQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Writes records to a PostgreSQL table. Each record will be written in a row in the table")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = PostgresConnector.NAME)})
public class PostgresSink extends AbstractDBSink<PostgresSink.PostgresSinkConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresSink.class);

  private static final Character ESCAPE_CHAR = '"';

  private final PostgresSinkConfig postgresSinkConfig;

  public PostgresSink(PostgresSinkConfig postgresSinkConfig) {
    super(postgresSinkConfig);
    this.postgresSinkConfig = postgresSinkConfig;
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

  @Override
  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    String fqn = DBUtils.constructFQN("postgres",
                                      postgresSinkConfig.getConnection().getHost(),
                                      postgresSinkConfig.getConnection().getPort(),
                                      postgresSinkConfig.getConnection().getDatabase(),
                                      postgresSinkConfig.getReferenceName());
    Asset asset = Asset.builder(postgresSinkConfig.getReferenceName()).setFqn(fqn).build();
    return new LineageRecorder(context, asset);
  }

  /**
   * PostgreSQL action configuration.
   */
  public static class PostgresSinkConfig extends AbstractDBSpecificSinkConfig {

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private PostgresConnectorConfig connection;

    @Name(PostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
    }

    @Override
    public String getEscapedTableName() {
      return ESCAPE_CHAR + getTableName() + ESCAPE_CHAR;
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(PostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }

    @Override
    protected PostgresConnectorConfig getConnection() {
      return connection;
    }
  }
}

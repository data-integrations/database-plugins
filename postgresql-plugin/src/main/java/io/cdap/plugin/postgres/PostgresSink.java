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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * Sink support for a PostgreSQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Writes records to a PostgreSQL table. Each record will be written in a row in the table")
public class PostgresSink extends AbstractDBSink {
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
  protected boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
      : field.getSchema().getType();

    String colTypeName = metadata.getColumnTypeName(index);
    int columnType = metadata.getColumnType(index);
    if (PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES_NAMES.contains(colTypeName) ||
      PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES.contains(columnType)) {
      if (Objects.equals(fieldType, Schema.Type.STRING)) {
        return true;
      } else {
        LOG.error("Field '{}' was given as type '{}' but must be of type 'string' for the PostgreSQL column of " +
                    "{} type.", field.getName(), fieldType, colTypeName);
        return false;
      }
    }

    return super.isFieldCompatible(field, metadata, index);
  }

  /**
   * PostgreSQL action configuration.
   */
  public static class PostgresSinkConfig extends DBSpecificSinkConfig {

    @Name(PostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String.format(PostgresConstants.POSTGRES_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    protected String getEscapedTableName() {
      return ESCAPE_CHAR + tableName + ESCAPE_CHAR;
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(PostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }
  }
}

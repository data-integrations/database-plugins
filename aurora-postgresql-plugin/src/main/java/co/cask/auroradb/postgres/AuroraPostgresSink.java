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

package co.cask.auroradb.postgres;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.db.batch.config.DBSpecificSinkConfig;
import co.cask.db.batch.sink.AbstractDBSink;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * Sink support for an Aurora DB PostgreSQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(AuroraPostgresConstants.PLUGIN_NAME)
@Description("Writes records to a table of Aurora DB PostgreSQL cluster. " +
  "Each record will be written in a row in the table.")
public class AuroraPostgresSink extends AbstractDBSink {

  private final AuroraPostgresSinkConfig auroraPostgresSinkConfig;

  public AuroraPostgresSink(AuroraPostgresSinkConfig auroraPostgresSinkConfig) {
    super(auroraPostgresSinkConfig);
    this.auroraPostgresSinkConfig = auroraPostgresSinkConfig;
  }

  @Override
  protected void setColumnsInfo(List<Schema.Field> fields) {
    List<String> columnsList = new ArrayList<>();
    StringJoiner columnsJoiner = new StringJoiner(",");

    for (Schema.Field field : fields) {
      columnsList.add(field.getName());
      columnsJoiner.add("\"" + field.getName() + "\"");
    }

    super.columns = Collections.unmodifiableList(columnsList);
    super.dbColumns = columnsJoiner.toString();
  }

  /**
   * Aurora DB PostgreSQL action configuration.
   */
  public static class AuroraPostgresSinkConfig extends DBSpecificSinkConfig {

    @Name(AuroraPostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String.format(AuroraPostgresConstants.AURORA_POSTGRES_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      if (connectionTimeout != null) {
        return ImmutableMap.of(AuroraPostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
      } else {
        return ImmutableMap.of();
      }
    }
  }
}
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

package co.cask.postgres;

import co.cask.SchemaReader;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.db.batch.config.DBSpecificSourceConfig;
import co.cask.db.batch.source.AbstractDBSource;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from PostgreSQL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class PostgresSource extends AbstractDBSource {

  private final PostgresSourceConfig postgresSourceConfig;

  public PostgresSource(PostgresSourceConfig postgresSourceConfig) {
    super(postgresSourceConfig);
    this.postgresSourceConfig = postgresSourceConfig;
  }

  @Override
  protected String createConnectionString(String host, Integer port, String database) {
    return String.format(PostgresConstants.POSTGRES_CONNECTION_STRING_FORMAT, host, port, database);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new PostgresSchemaReader();
  }

  /**
   * PosgtreSQL source config.
   */
  public static class PostgresSourceConfig extends DBSpecificSourceConfig {

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
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(PostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }
  }
}

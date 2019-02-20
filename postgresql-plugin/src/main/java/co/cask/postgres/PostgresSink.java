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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.db.batch.config.DBSpecificSinkConfig;
import co.cask.db.batch.sink.AbstractDBSink;
import com.google.common.collect.ImmutableMap;

import java.util.Map;


/**
 * Sink support for a PostgreSQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Writes records to a PostgreSQL table. Each record will be written in a row in the table")
public class PostgresSink extends AbstractDBSink {

  private final PostgresSinkConfig postgresSinkConfig;

  public PostgresSink(PostgresSinkConfig postgresSinkConfig) {
    super(postgresSinkConfig);
    this.postgresSinkConfig = postgresSinkConfig;
  }

  /**
   * PostgreSQL action configuration.
   */
  public static class PostgresSinkConfig extends DBSpecificSinkConfig {

    @Name(PostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
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

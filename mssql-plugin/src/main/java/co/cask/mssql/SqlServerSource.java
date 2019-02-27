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

package co.cask.mssql;

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
 * Batch source to read from MSSQL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SqlServerConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class SqlServerSource extends AbstractDBSource {

  private final SqlServerSourceConfig sqlServerSourceConfig;

  public SqlServerSource(SqlServerSourceConfig sqlServerSourceConfig) {
    super(sqlServerSourceConfig);
    this.sqlServerSourceConfig = sqlServerSourceConfig;
  }

  @Override
  protected String createConnectionString(String host, Integer port, String database) {
    return String.format(SqlServerConstants.SQL_SERVER_CONNECTION_STRING_FORMAT, host, port, database);
  }

  /**
   * MSSQL source config.
   */
  public static class SqlServerSourceConfig extends DBSpecificSourceConfig {

    @Name(SqlServerConstants.INSTANCE_NAME)
    @Description("The SQL Server instance name to connect to. When it is not specified, a connection is made" +
      " to the default instance. For the case where both the instanceName " +
      "and port are specified, see the notes for port.")
    @Nullable
    public String instanceName;

    @Name(SqlServerConstants.QUERY_TIMEOUT)
    @Description("The number of seconds to wait before a timeout has occurred on a query. The default value is -1, " +
      "which means infinite timeout. Setting this to 0 also implies to wait indefinitely.")
    @Nullable
    public Integer queryTimeout = -1;

    @Override
    public String getConnectionString() {
      return String.format(SqlServerConstants.SQL_SERVER_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      if (instanceName != null) {
        builder.put(SqlServerConstants.INSTANCE_NAME, String.valueOf(instanceName));
      }

      return builder.put(SqlServerConstants.QUERY_TIMEOUT, String.valueOf(queryTimeout)).build();
    }
  }
}

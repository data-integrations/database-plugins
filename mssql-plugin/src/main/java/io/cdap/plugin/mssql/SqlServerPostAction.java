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

package io.cdap.plugin.mssql;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.db.batch.action.AbstractQueryAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryActionConfig;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents MSSQL post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(SqlServerConstants.PLUGIN_NAME)
@Description("Runs a MSSQL query after a pipeline run.")
public class SqlServerPostAction extends AbstractQueryAction {

  private final SqlServerQueryActionConfig sqlServerQueryActionConfig;

  public SqlServerPostAction(SqlServerQueryActionConfig sqlServerQueryActionConfig) {
    super(sqlServerQueryActionConfig, false);
    this.sqlServerQueryActionConfig = sqlServerQueryActionConfig;
  }

  /**
   * MSSQL post action configuration.
   */
  public static class SqlServerQueryActionConfig extends DBSpecificQueryActionConfig {

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

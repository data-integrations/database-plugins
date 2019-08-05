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

package io.cdap.plugin.jdbc;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.batch.action.AbstractQueryAction;
import io.cdap.plugin.db.batch.action.QueryActionConfig;

import javax.annotation.Nullable;

/**
 * Represents database post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(DatabaseConstants.PLUGIN_NAME)
@Description("Runs a MySQL query after a pipeline run.")
public class DatabasePostAction extends AbstractQueryAction {

  public DatabaseQueryActionConfig databaseQueryActionConfig;

  public DatabasePostAction(DatabaseQueryActionConfig databaseQueryActionConfig) {
    super(databaseQueryActionConfig, databaseQueryActionConfig.enableAutoCommit);
    this.databaseQueryActionConfig = databaseQueryActionConfig;
  }

  /**
   * Database post action databaseQueryActionConfig.
   */
  public static class DatabaseQueryActionConfig extends QueryActionConfig {
    @Name(ConnectionConfig.CONNECTION_STRING)
    @Description("JDBC connection string including database name.")
    @Macro
    public String connectionString;

    @Name(ENABLE_AUTO_COMMIT)
    @Description("Whether to enable auto commit for queries run by this source. Defaults to false. " +
      "This setting should only matter if you are using a jdbc driver that does not support a false value for " +
      "auto commit, or a driver that does not support the commit call. For example, the Hive jdbc driver will throw " +
      "an exception whenever a commit is called. For drivers like that, this should be set to true.")
    @Nullable
    public Boolean enableAutoCommit;

    @Override
    public String getConnectionString() {
      return connectionString;
    }
  }
}

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

package co.cask.jdbc;

import co.cask.ConnectionConfig;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.db.batch.action.AbstractQueryAction;
import co.cask.db.batch.action.QueryActionConfig;

import javax.annotation.Nullable;

/**
 * Represents database post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("DatabaseQuery")
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

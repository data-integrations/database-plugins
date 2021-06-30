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
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;

import javax.annotation.Nullable;

/**
 * Batch source to read from generic database.
 */
@Plugin(type = "batchsource")
@Name(DatabaseConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class DatabaseSource extends AbstractDBSource<DatabaseSource.DatabaseSourceConfig> {

  private final DatabaseSourceConfig databaseSourceConfig;

  public DatabaseSource(DatabaseSourceConfig databaseSourceConfig) {
    super(databaseSourceConfig);
    this.databaseSourceConfig = databaseSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return databaseSourceConfig.connectionString;
  }

  /**
   * Generic database source configuration.
   */
  public static class DatabaseSourceConfig extends AbstractDBSource.DBSourceConfig {
    @Name(ConnectionConfig.CONNECTION_STRING)
    @Description("JDBC connection string including database name.")
    @Macro
    public String connectionString;

    @Nullable
    @Name(TRANSACTION_ISOLATION_LEVEL)
    @Description("The transaction isolation level for queries run by this sink. " +
      "Defaults to TRANSACTION_SERIALIZABLE. See java.sql.Connection#setTransactionIsolation for more details. " +
      "The Phoenix jdbc driver will throw an exception if the Phoenix database does not have transactions enabled " +
      "and this setting is set to true. For drivers like that, this should be set to TRANSACTION_NONE.")
    @Macro
    public String transactionIsolationLevel;

    @Override
    public String getConnectionString() {
      return connectionString;
    }
  }
}

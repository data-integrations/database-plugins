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
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;

import javax.annotation.Nullable;

/**
 * Sink support for a generic database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(DatabaseConstants.PLUGIN_NAME)
@Description("Writes records to a database table. Each record will be written in a row in the table")
public class DatabaseSink extends AbstractDBSink<DatabaseSink.DatabaseSinkConfig> {

  private final DatabaseSinkConfig databaseSinkConfig;

  public DatabaseSink(DatabaseSinkConfig databaseSinkConfig) {
    super(databaseSinkConfig);
    this.databaseSinkConfig = databaseSinkConfig;
  }

  /**
   * Generic database sink configuration.
   */
  public static class DatabaseSinkConfig extends AbstractDBSink.DBSinkConfig {
    @Name(ConnectionConfig.CONNECTION_STRING)
    @Description("JDBC connection string including database name.")
    @Macro
    public String connectionString;

    @Name("Reference Name")
    @Description("FQN for Database Sink")
    @Macro
    public String referenceName;

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

    @Override
    public String getTransactionIsolationLevel() {
      return transactionIsolationLevel;
    }

    @Override
    public String getReferenceName() {
      referenceName = DatabaseURLParser.getFQN(connectionString);
      return referenceName;
    }
  }
}

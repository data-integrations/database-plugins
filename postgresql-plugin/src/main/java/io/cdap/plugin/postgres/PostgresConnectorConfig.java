/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;

/**
 * Configuration for Postgres SQL Database Connector
 */
public class PostgresConnectorConfig extends AbstractDBSpecificConnectorConfig {
  public static final String NAME_DATABASE = "database";
  public PostgresConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                                 String connectionArguments) {

    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
  }

  @Name(NAME_DATABASE)
  @Description("Database to connect to.")
  private String database;

  @Override
  public String getConnectionString() {
    return String.format(PostgresConstants.POSTGRES_CONNECTION_STRING_WITH_DB_FORMAT, host, getPort(), database);
  }

  public String getDatabase() {
    return database;
  }
  @Override
  protected int getDefaultPort() {
    return 5432;
  }

  @Override
  public boolean canConnect() {
    return super.canConnect() && !containsMacro(NAME_DATABASE);
  }
}

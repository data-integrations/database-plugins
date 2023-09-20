/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.amazon.redshift;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;

import javax.annotation.Nullable;

/**
 * Configuration for Redshift connector
 */
public class RedshiftConnectorConfig extends AbstractDBConnectorConfig {

  @Name(ConnectionConfig.HOST)
  @Description(
    "The endpoint of the Amazon Redshift cluster.")
  @Macro
  private String host;

  @Name(ConnectionConfig.PORT)
  @Description("Database port number")
  @Macro
  @Nullable
  private Integer port;

  @Name(ConnectionConfig.DATABASE)
  @Description("Database name to connect to")
  @Macro
  private String database;

  public RedshiftConnectorConfig(String username, String password, String jdbcPluginName,
                                 String connectionArguments, String host,
                                 String database, @Nullable Integer port) {
    this.user = username;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
    this.host = host;
    this.database = database;
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port == null ? 5439 : port;
  }

  @Override
  public String getConnectionString() {
      return String.format(
        RedshiftConstants.REDSHIFT_CONNECTION_STRING_FORMAT,
        host,
        getPort(),
        database);
  }

  @Override
  public boolean canConnect() {
    return super.canConnect() && !containsMacro(ConnectionConfig.HOST) &&
        !containsMacro(ConnectionConfig.PORT) && !containsMacro(ConnectionConfig.DATABASE);
  }
}

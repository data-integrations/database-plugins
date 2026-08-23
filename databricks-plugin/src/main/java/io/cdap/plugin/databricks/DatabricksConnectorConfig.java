/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.databricks;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;

import javax.annotation.Nullable;

/**
 * Configuration for Databricks connector
 */
public class DatabricksConnectorConfig extends AbstractDBConnectorConfig {

  public static final String HTTP_PATH = "httpPath";

  @Name(ConnectionConfig.HOST)
  @Description("The server hostname of the Databricks cluster or SQL warehouse.")
  @Macro
  private String host;

  @Name(ConnectionConfig.PORT)
  @Description("Database port number. Default is 443.")
  @Macro
  @Nullable
  private Integer port;

  @Name(HTTP_PATH)
  @Description("The HTTP Path for the Databricks cluster or SQL warehouse.")
  @Macro
  private String httpPath;

  @Name(ConnectionConfig.DATABASE)
  @Description("Database or Catalog name to connect to.")
  @Macro
  @Nullable
  private String database;

  public DatabricksConnectorConfig(@Nullable @Name(ConnectionConfig.USER) String user, 
                                  @Nullable @Name(ConnectionConfig.PASSWORD) String password, 
                                  @Name(ConnectionConfig.JDBC_PLUGIN_NAME) String jdbcPluginName,
                                  @Nullable @Name(ConnectionConfig.CONNECTION_ARGUMENTS) String connectionArguments, 
                                  @Name(ConnectionConfig.HOST) String host, 
                                  @Name(DatabricksConnectorConfig.HTTP_PATH) String httpPath,
                                  @Nullable @Name(ConnectionConfig.DATABASE) String database, 
                                  @Nullable @Name(ConnectionConfig.PORT) Integer port) {
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
    this.host = host;
    this.httpPath = httpPath;
    this.database = database;
    this.port = port;
  }

  @Nullable
  @Override
  public String getUser() {
    if (Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(password)) {
      return "token";
    }
    return user;
  }

  @Override
  public java.util.Properties getConnectionArgumentsProperties() {
    return getConnectionArgumentsProperties(connectionArguments, getUser(), getPassword());
  }

  @Nullable
  public String getDatabase() {
    return database;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port == null ? 443 : port;
  }

  public String getHttpPath() {
    return httpPath;
  }

  @Override
  public String getConnectionString() {
    if (database != null && !database.trim().isEmpty()) {
      return String.format(
        DatabricksConstants.DATABRICKS_DB_CONNECTION_STRING_FORMAT,
        host,
        getPort(),
        database,
        httpPath);
    }
    return String.format(
      DatabricksConstants.DATABRICKS_CONNECTION_STRING_FORMAT,
      host,
      getPort(),
      httpPath);
  }

  @Override
  public boolean canConnect() {
    return super.canConnect() && !containsMacro(ConnectionConfig.HOST) &&
      !containsMacro(ConnectionConfig.PORT) && !containsMacro(HTTP_PATH) &&
      !containsMacro(ConnectionConfig.DATABASE);
  }
}

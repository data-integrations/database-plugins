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

package io.cdap.plugin.mssql;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Configuration for SqlServer Database Connector
 */
public class SqlServerConnectorConfig extends AbstractDBSpecificConnectorConfig {

  private static final String SQLSERVER_CONNECTION_STRING_FORMAT = "jdbc:sqlserver://%s:%s";

  @Name(SqlServerConstants.AUTHENTICATION)
  @Description(SqlServerConstants.AUTHENTICATION_DESCRIPTION)
  @Nullable
  public String authenticationType;

  public SqlServerConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                              String connectionArguments) {

    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
  }

  @Override
  public String getConnectionString() {
    return String.format(SQLSERVER_CONNECTION_STRING_FORMAT, host, getPort());
  }

  @Override
  public Properties getConnectionArgumentsProperties() {
    Properties arguments = getConnectionArgumentsProperties(connectionArguments, user, password);
    if (SqlServerConstants.AD_PASSWORD_OPTION.equals(authenticationType)) {
      arguments.put(SqlServerConstants.AUTHENTICATION, SqlServerConstants.AD_PASSWORD_OPTION);
    }
    return arguments;
  }

  @Override
  protected int getDefaultPort() {
    return 1433;
  }

  public String getAuthenticationType() {
    return authenticationType;
  }
}

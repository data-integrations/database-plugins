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

package io.cdap.plugin.oracle;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;

/**
 * Configuration for Oracle Database Connector
 */
public class OracleConnectorConfig extends AbstractDBSpecificConnectorConfig {

  private static final String ORACLE_CONNECTION_STRING_SID_FORMAT = "jdbc:oracle:thin:@%s:%s";
  private static final String ORACLE_CONNECTION__STRING_SERVICE_NAME_FORMAT = "jdbc:oracle:thin:@//%s:%s";

  public OracleConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                               String connectionArguments) {
    this(host, port, user, password, jdbcPluginName, connectionArguments, null);
  }

  public OracleConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                               String connectionArguments, String connectionType) {

    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
    this.connectionType = connectionType;
  }

  @Override
  public String getConnectionString() {
    if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(connectionType)) {
      return String.format(ORACLE_CONNECTION__STRING_SERVICE_NAME_FORMAT, host, getPort());
    }
    return String
      .format(ORACLE_CONNECTION_STRING_SID_FORMAT, host, getPort());
  }

  @Name(OracleConstants.CONNECTION_TYPE)
  @Description("Whether to use an SID or Service Name when connecting to the database.")
  public String connectionType;

  @Override
  protected int getDefaultPort() {
    return 1521;
  }

  public String getConnectionType() {
    return connectionType;
  }
}

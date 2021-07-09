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

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Configuration for Oracle Database Connector
 */
public class OracleConnectorConfig extends AbstractDBSpecificConnectorConfig {

  private static final String ORACLE_CONNECTION_STRING_SID_WITHOUT_DB_FORMAT = "jdbc:oracle:thin:@%s:%s";
  private static final String ORACLE_CONNECTION_STRING_SERVICE_NAME_WITHOUT_DB_FORMAT = "jdbc:oracle:thin:@//%s:%s";
  private static final String TIME_ZONE_AS_REGION_PROPERTY = "oracle.jdbc.timezoneAsRegion";
  private static final String INTERNAL_LOGON_PROPERTY = "internal_logon";

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
      return String.format(ORACLE_CONNECTION_STRING_SERVICE_NAME_WITHOUT_DB_FORMAT, host, getPort());
    }
    return String.format(ORACLE_CONNECTION_STRING_SID_WITHOUT_DB_FORMAT, host, getPort());
  }

  @Name(OracleConstants.CONNECTION_TYPE)
  @Description("Whether to use an SID or Service Name when connecting to the database.")
  private String connectionType;

  @Name(OracleConstants.ROLE)
  @Description("Login role of the user when connecting to the database.")
  @Nullable
  private String role;

  @Override
  protected int getDefaultPort() {
    return 1521;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public String getRole() {
    return role == null ? "normal" : role;
  }

  @Override
  public Properties getConnectionArgumentsProperties() {
    Properties prop = super.getConnectionArgumentsProperties();
    // To solve the "ORA-01882: timezone region not found" issue, see:
    // https://stackoverflow.com/questions/9156379/ora-01882-timezone-region-not-found
    prop.put(TIME_ZONE_AS_REGION_PROPERTY, "false");
    prop.put(INTERNAL_LOGON_PROPERTY, getRole());
    return prop;
  }
}

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

package io.cdap.plugin.cloudsql.mysql;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;
import io.cdap.plugin.util.CloudSQLUtil;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Configuration for CloudSQL MySQL connector
 */
public class CloudSQLMySQLConnectorConfig extends AbstractDBConnectorConfig {

  private static final String JDBC_PROPERTY_CONNECT_TIMEOUT_MILLIS = "connectTimeout";
  private static final String JDBC_PROPERTY_SOCKET_TIMEOUT_MILLIS = "socketTimeout";

  @Name(CloudSQLUtil.CONNECTION_NAME)
  @Description(
    "The CloudSQL instance to connect to. For a public instance, the connection string should be in the format "
      + "<PROJECT_ID>:<REGION>:<INSTANCE_NAME> which can be found in the instance overview page. For a private "
      + "instance, enter the internal IP address of the Compute Engine VM cloudsql proxy is running on.")
  @Macro
  private String connectionName;

  @Name(ConnectionConfig.PORT)
  @Description("Database port number")
  @Macro
  @Nullable
  private Integer port;

  @Name(ConnectionConfig.DATABASE)
  @Description("Database name to connect to")
  @Macro
  private String database;

  @Name(CloudSQLUtil.INSTANCE_TYPE)
  @Description("Whether the CloudSQL instance to connect to is private or public.")
  private String instanceType;

  public CloudSQLMySQLConnectorConfig(String user, String password, String jdbcPluginName, String connectionArguments,
                                      String instanceType, String connectionName, String database,
                                      @Nullable Integer port) {
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
    this.instanceType = instanceType;
    this.connectionName = connectionName;
    this.database = database;
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public String getInstanceType() {
    return instanceType;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public int getPort() {
    return port == null ? 3306 : port;
  }

  @Override
  public String getConnectionString() {
    if (CloudSQLUtil.PRIVATE_INSTANCE.equalsIgnoreCase(instanceType)) {
      return String.format(
        CloudSQLMySQLConstants.PRIVATE_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
        connectionName,
        getPort(),
        database);
    }

    return String.format(
      CloudSQLMySQLConstants.PUBLIC_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
      database,
      connectionName);
  }

  @Override
  public Properties getConnectionArgumentsProperties() {
    Properties properties = super.getConnectionArgumentsProperties();
    properties.put(JDBC_PROPERTY_CONNECT_TIMEOUT_MILLIS, "20000");
    properties.put(JDBC_PROPERTY_SOCKET_TIMEOUT_MILLIS, "20000");
    return properties;
  }

  @Override
  public boolean canConnect() {
    return super.canConnect() && !containsMacro(CloudSQLUtil.CONNECTION_NAME) &&
        !containsMacro(ConnectionConfig.PORT) && !containsMacro(ConnectionConfig.DATABASE);
  }
}

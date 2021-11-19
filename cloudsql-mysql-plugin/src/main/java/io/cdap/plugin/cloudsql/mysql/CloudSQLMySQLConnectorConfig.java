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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Configuration for CloudSQL MySQL connector
 */
public class CloudSQLMySQLConnectorConfig extends AbstractDBConnectorConfig {

  private static final String JDBC_PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  private static final String JDBC_PROPERTY_SOCKET_TIMEOUT = "socketTimeout";

  @Name(CloudSQLMySQLConstants.CONNECTION_NAME)
  @Description(
    "The CloudSQL instance to connect to. For a public instance, the connection string should be in the format "
      + "<PROJECT_ID>:<REGION>:<INSTANCE_NAME> which can be found in the instance overview page. For a private "
      + "instance, enter the internal IP address of the Compute Engine VM cloudsql proxy is running on.")
  private String connectionName;

  @Name(ConnectionConfig.DATABASE)
  @Description("Database name to connect to")
  private String database;

  @Name(CloudSQLMySQLConstants.INSTANCE_TYPE)
  @Description("Whether the CloudSQL instance to connect to is private or public.")
  @Nullable
  private String instanceType;

  public String getDatabase() {
    return database;
  }

  public String getInstanceType() {
    return instanceType;
  }

  public String getConnectionName() {
    return connectionName;
  }

  @Override
  public String getConnectionString() {
    if (CloudSQLMySQLConstants.PRIVATE_INSTANCE.equalsIgnoreCase(instanceType)) {
      return String.format(
        CloudSQLMySQLConstants.PRIVATE_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT,
        connectionName,
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
    // the unit below is milli-second
    properties.put(JDBC_PROPERTY_CONNECT_TIMEOUT, "20000");
    properties.put(JDBC_PROPERTY_SOCKET_TIMEOUT, "20000");
    return properties;
  }
}

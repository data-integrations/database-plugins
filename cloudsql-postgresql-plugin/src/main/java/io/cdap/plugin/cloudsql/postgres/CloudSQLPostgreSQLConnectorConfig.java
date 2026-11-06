/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.cloudsql.postgres;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;
import io.cdap.plugin.util.CloudSQLUtil;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Configuration for CloudSQL PostgreSQL connector
 */
public class CloudSQLPostgreSQLConnectorConfig extends AbstractDBConnectorConfig {

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

  @Name(CloudSQLUtil.AUTHENTICATION_TYPE)
  @Description("How to authenticate to the database. 'basic' uses the username and password. 'iam' uses Cloud SQL "
    + "IAM database authentication as the service account in Service Account Email.")
  @Nullable
  private String authenticationType;

  @Name(CloudSQLUtil.SERVICE_ACCOUNT_EMAIL)
  @Description("Email of the IAM principal to log in as when the authentication type is 'iam'. For a public "
    + "instance, this is the service account that Data Fusion impersonates. For a private instance, this is the "
    + "identity the Cloud SQL Auth Proxy runs as. The database username is derived from this email.")
  @Macro
  @Nullable
  private String serviceAccountEmail;

  public CloudSQLPostgreSQLConnectorConfig(String username, String password, String jdbcPluginName,
                                           String connectionArguments, String instanceType,
                                           String connectionName, String database, @Nullable Integer port) {
    this.user = username;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
    this.instanceType = instanceType;
    this.connectionName = connectionName;
    this.database = database;
    this.port = port;
  }

  public CloudSQLPostgreSQLConnectorConfig(String username, String password, String jdbcPluginName,
                                           String connectionArguments, String instanceType,
                                           String connectionName, String database, @Nullable Integer port,
                                           @Nullable String authenticationType, @Nullable String serviceAccountEmail) {
    this(username, password, jdbcPluginName, connectionArguments, instanceType, connectionName, database, port);
    this.authenticationType = authenticationType;
    this.serviceAccountEmail = serviceAccountEmail;
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

  @Nullable
  public String getAuthenticationType() {
    return authenticationType;
  }

  @Nullable
  public String getServiceAccountEmail() {
    return serviceAccountEmail;
  }

  public boolean isIamAuthentication() {
    return CloudSQLUtil.isIamAuthentication(authenticationType);
  }

  @Nullable
  @Override
  public String getUser() {
    return isIamAuthentication()
      ? CloudSQLUtil.getIamDatabaseUser(serviceAccountEmail, CloudSQLUtil.CLOUDSQL_POSTGRESQL) : super.getUser();
  }

  @Nullable
  @Override
  public String getPassword() {
    return isIamAuthentication() ? CloudSQLUtil.getIamPassword(instanceType) : super.getPassword();
  }

  /**
   * Validates the authentication properties.
   *
   * @param collector failure collector
   */
  public void validateAuthentication(FailureCollector collector) {
    CloudSQLUtil.validateAuthentication(collector, authenticationType, instanceType, serviceAccountEmail,
                                        containsMacro(CloudSQLUtil.SERVICE_ACCOUNT_EMAIL));
  }

  public int getPort() {
    return port == null ? 5432 : port;
  }

  @Override
  public String getConnectionString() {
    if (CloudSQLUtil.PRIVATE_INSTANCE.equalsIgnoreCase(instanceType)) {
      return String.format(
        CloudSQLPostgreSQLConstants.PRIVATE_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
        connectionName,
        getPort(),
        database);
    }

    return String.format(
      CloudSQLPostgreSQLConstants.PUBLIC_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
      database,
      connectionName);
  }

  @Override
  public Properties getConnectionArgumentsProperties() {
    Properties properties = super.getConnectionArgumentsProperties();
    if (isIamAuthentication()) {
      properties.putAll(CloudSQLUtil.getIamConnectionArguments(instanceType, serviceAccountEmail));
    }
    return properties;
  }

  @Override
  public boolean canConnect() {
    return super.canConnect() && !containsMacro(CloudSQLUtil.CONNECTION_NAME) &&
        !containsMacro(ConnectionConfig.PORT) && !containsMacro(ConnectionConfig.DATABASE) &&
        !containsMacro(CloudSQLUtil.SERVICE_ACCOUNT_EMAIL);
  }
}

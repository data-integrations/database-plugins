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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;

import javax.annotation.Nullable;

/**
 * Configuration for CloudSQL PostgreSQL connector
 */
public class CloudSQLPostgreSQLConnectorConfig extends AbstractDBConnectorConfig {

  @Name(CloudSQLPostgreSQLConstants.CONNECTION_NAME)
  @Description(
    "The CloudSQL instance to connect to. For a public instance, the connection string should be in the format "
      + "<PROJECT_ID>:<REGION>:<INSTANCE_NAME> which can be found in the instance overview page. For a private "
      + "instance, enter the internal IP address of the Compute Engine VM cloudsql proxy is running on.")
  private String connectionName;

  @Name(ConnectionConfig.DATABASE)
  @Description("Database name to connect to")
  private String database;

  @Name(CloudSQLPostgreSQLConstants.INSTANCE_TYPE)
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
    if (CloudSQLPostgreSQLConstants.PRIVATE_INSTANCE.equalsIgnoreCase(instanceType)) {
      return String.format(
        CloudSQLPostgreSQLConstants.PRIVATE_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
        connectionName,
        database);
    }

    return String.format(
      CloudSQLPostgreSQLConstants.PUBLIC_CLOUDSQL_POSTGRES_CONNECTION_STRING_FORMAT,
      database,
      connectionName);
  }
}

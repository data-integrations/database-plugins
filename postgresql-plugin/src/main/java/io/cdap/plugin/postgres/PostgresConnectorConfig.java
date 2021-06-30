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

import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;

/**
 * Configuration for Postgres SQL Database Connector
 */
public class PostgresConnectorConfig extends AbstractDBSpecificConnectorConfig {
  private static final String POSTGRE_CONNECTION_STRING_WITHOUT_DB_FORMAT = "jdbc:postgresql://%s:%s/";
  public PostgresConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
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
    return String.format(POSTGRE_CONNECTION_STRING_WITHOUT_DB_FORMAT, host, getPort());
  }

  @Override
  protected int getDefaultPort() {
    return 5432;
  }
}

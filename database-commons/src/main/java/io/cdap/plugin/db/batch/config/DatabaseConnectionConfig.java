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

package io.cdap.plugin.db.batch.config;

import java.util.Map;

/**
 * Interface for DB connection config
 */
public interface DatabaseConnectionConfig {

  boolean containsMacro(String property);

  /**
   * Constructs a connection string from host, port and database properties in a database-specific format.
   * @return connection string specific to a particular database.
   */
  String getConnectionString();

  /**
   * Parses connection arguments into a {@link Map}.
   * @return a {@link Map} of connection arguments, parsed from the config.
   */
  Map<String, String> getConnectionArguments();

  /**
   * @return the name of the jdbc plugin used to connect to the database
   */
  String getJdbcPluginName();

  /**
   * @return the user name used to connect to the database
   */
  String getUser();

  /**
   * @return the password used to connect to the database
   */
  String getPassword();

}

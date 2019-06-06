/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.db;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.KeyValueListParser;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Database source, sink, and action can all re-use.
 */
public abstract class ConnectionConfig extends PluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String ENABLE_AUTO_COMMIT = "enableAutoCommit";
  public static final String USER = "user";
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String DATABASE = "database";
  public static final String PASSWORD = "password";
  public static final String CONNECTION_ARGUMENTS = "connectionArguments";
  public static final String JDBC_PLUGIN_NAME = "jdbcPluginName";
  public static final String JDBC_PLUGIN_TYPE = "jdbc";

  @Name(JDBC_PLUGIN_NAME)
  @Description("Name of the JDBC driver to use. This is the value of the 'jdbcPluginName' key defined in the JSON " +
    "file for the JDBC plugin.")
  public String jdbcPluginName;

  @Name(USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String password;

  @Name(CONNECTION_ARGUMENTS)
  @Description("A list of arbitrary string key/value pairs as connection arguments.")
  @Nullable
  @Macro
  public String connectionArguments;

  public ConnectionConfig() {
  }

  /**
   * Parses connection arguments into a {@link Map}.
   * @return a {@link Map} of connection arguments, parsed from the config.
   */
  public Map<String, String> getConnectionArguments() {
    Map<String, String> arguments = getConnectionArguments(this.connectionArguments, user, password);
    arguments.putAll(getDBSpecificArguments());
    return arguments;
  }

  /**
   * Constructs a connection string from host, port and database properties in a database-specific format.
   * @return connection string specific to a particular database.
   */
  public abstract String getConnectionString();

  /**
   * Returns list of initialization queries. Initialization queries supposed to be executed preserving order right after
   * connection establishing. In the case when there are no initialization queries, an empty list will be returned.
   * @return list of initialization queries.
   */
  public List<String> getInitQueries() {
    return Collections.emptyList();
  }

  /**
   * Parses connection arguments into a {@link Map}.
   *
   * @param connectionArguments See {@link ConnectionConfig#connectionArguments}.
   * @param user                See {@link ConnectionConfig#user}.
   * @param password            See {@link ConnectionConfig#password}.
   */
  protected static Map<String, String> getConnectionArguments(@Nullable String connectionArguments,
                                                              @Nullable String user, @Nullable String password) {
    KeyValueListParser kvParser = new KeyValueListParser("\\s*;\\s*", "=");

    Map<String, String> connectionArgumentsMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(connectionArguments)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(connectionArguments)) {
        connectionArgumentsMap.put(keyVal.getKey(), keyVal.getValue());
      }
    }

    if (user != null) {
      connectionArgumentsMap.put("user", user);
      connectionArgumentsMap.put("password", password);
    }

    return connectionArgumentsMap;
  }

  /**
   * Provides support for database-specific configuration properties.
   * @return {@link Map} of additional connection arguments.
   */
  protected Map<String, String> getDBSpecificArguments() {
    return Collections.emptyMap();
  }

}

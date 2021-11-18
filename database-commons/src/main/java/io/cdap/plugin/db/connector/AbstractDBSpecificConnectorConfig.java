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

package io.cdap.plugin.db.connector;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.db.DBConnectorProperties;
import io.cdap.plugin.db.ConnectionConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * An abstract DB Specific Connector config that those DB specific plugin config can inherit
 */
public abstract class AbstractDBSpecificConnectorConfig extends PluginConfig implements DBConnectorProperties {

  @Name(ConnectionConfig.HOST)
  @Description("Database host")
  @Macro
  @Nullable
  protected String host;

  @Name(ConnectionConfig.PORT)
  @Description("Database port number")
  @Macro
  @Nullable
  protected Integer port;

  @Name(ConnectionConfig.JDBC_PLUGIN_NAME)
  @Description("Name of the JDBC driver to use. This is the value of the 'jdbcPluginName' key defined in the JSON " +
    "file for the JDBC plugin.")
  @Nullable
  protected String jdbcPluginName;

  @Name(ConnectionConfig.USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  protected String user;

  @Name(ConnectionConfig.PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  protected String password;

  @Name(ConnectionConfig.CONNECTION_ARGUMENTS)
  @Description("A list of arbitrary string key/value pairs as connection arguments.")
  @Nullable
  @Macro
  protected String connectionArguments;

  @Nullable
  @Override
  public String getUser() {
    return user;
  }

  @Nullable
  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public Properties getConnectionArgumentsProperties() {
    return getConnectionArgumentsProperties(connectionArguments, user, password);
  }

  @Override
  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

  @Override
  public String getConnectionArguments() {
    return connectionArguments;
  }

  protected static Properties getConnectionArgumentsProperties(@Nullable String connectionArguments,
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
    }
    if (password != null) {
      connectionArgumentsMap.put("password", password);
    }
    Properties properties = new Properties();
    properties.putAll(connectionArgumentsMap);
    return properties;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port == null ? getDefaultPort() : port;
  }

  protected abstract int getDefaultPort();

  public String getTransactionIsolationLevel() {
    return null;
  }

  public boolean canConnect() {
    return !containsMacro(ConnectionConfig.HOST) && !containsMacro(ConnectionConfig.PORT)
      && !containsMacro(ConnectionConfig.USER) && !containsMacro(ConnectionConfig.PASSWORD);
  }
}

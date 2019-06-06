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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.plugin.db.batch.TransactionIsolationLevel;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Allows to specify and access connection configuration properties of {@link Configuration}.
 */
public class ConnectionConfigAccessor {

  public static final String OVERRIDE_SCHEMA = "io.cdap.plugin.db.override.schema";
  private static final String CONNECTION_ARGUMENTS = "io.cdap.plugin.db.connection.arguments";
  private static final String INIT_QUERIES = "io.cdap.plugin.db.init.queries";
  public static final String AUTO_COMMIT_ENABLED = "io.cdap.plugin.db.output.autocommit.enabled";

  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

  private final Configuration configuration;


  public ConnectionConfigAccessor() {
    this.configuration = new Configuration();
    configuration.clear();
  }

  public ConnectionConfigAccessor(Configuration configuration) {
    this.configuration = configuration;
  }

  public void setTransactionIsolationLevel(String transactionIsolationLevel) {
    configuration.set(TransactionIsolationLevel.CONF_KEY, transactionIsolationLevel);
  }

  public String getTransactionIsolationLevel() {
    return configuration.get(TransactionIsolationLevel.CONF_KEY);
  }

  public void setConnectionArguments(Map<String, String> connectionArguments) {
    configuration.set(CONNECTION_ARGUMENTS, GSON.toJson(connectionArguments, STRING_MAP_TYPE));
  }

  public Map<String, String> getConnectionArguments() {
    if (Strings.isNullOrEmpty(configuration.get(CONNECTION_ARGUMENTS))) {
      return Collections.emptyMap();
    }
    return GSON.fromJson(configuration.get(CONNECTION_ARGUMENTS), STRING_MAP_TYPE);
  }

  public void setInitQueries(List<String> initQueries) {
    configuration.set(INIT_QUERIES, GSON.toJson(initQueries, STRING_LIST_TYPE));
  }

  public List<String> getInitQueries() {
    if (Strings.isNullOrEmpty(configuration.get(INIT_QUERIES))) {
      return Collections.emptyList();
    }
    return GSON.fromJson(configuration.get(INIT_QUERIES), STRING_LIST_TYPE);
  }

  public void setSchema(String schema) {
    configuration.set(OVERRIDE_SCHEMA, schema);
  }

  public String getSchema() {
    return configuration.get(OVERRIDE_SCHEMA);
  }

  public void setAutoCommitEnabled(boolean enableAutoCommit) {
    configuration.setBoolean(AUTO_COMMIT_ENABLED, enableAutoCommit);
  }

  public boolean isAutoCommitEnabled() {
    return configuration.getBoolean(AUTO_COMMIT_ENABLED, false);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

}

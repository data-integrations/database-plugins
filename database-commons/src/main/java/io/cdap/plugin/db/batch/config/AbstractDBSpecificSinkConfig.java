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

import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract Config for DB Specific Sink plugin
 */
public abstract class AbstractDBSpecificSinkConfig extends PluginConfig implements DatabaseSinkConfig {
  public static final String TABLE_NAME = "tableName";
  public static final String DB_SCHEMA_NAME = "dbSchemaName";
  public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Name(TABLE_NAME)
  @Description("Name of the database table to write to.")
  @Macro
  private String tableName;

  @Name(DB_SCHEMA_NAME)
  @Description("Name of the database schema of table.")
  @Macro
  @Nullable
  private String dbSchemaName;

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public String getDBSchemaName() {
    return dbSchemaName;
  }

  /**
   * Adds escape characters (back quotes, double quotes, etc.) to the table name for
   * databases with case-sensitive identifiers.
   *
   * @return tableName with leading and trailing escape characters appended.
   * Default implementation returns unchanged table name string.
   */
  @Override
  public String getEscapedTableName() {
    return tableName;
  }

  @Override
  public boolean canConnect() {
    return !containsMacro(TABLE_NAME) && getConnection().canConnect();
  }

  @Nullable
  @Override
  public String getTransactionIsolationLevel() {
    return null;
  }

  @Override
  public List<String> getInitQueries() {
    return Collections.emptyList();
  }

  protected abstract Map<String, String> getDBSpecificArguments();

  protected abstract AbstractDBConnectorConfig getConnection();

  @Override
  public String getConnectionString() {
    return getConnection().getConnectionString();
  }

  @Override
  public Map<String, String> getConnectionArguments() {
    Map<String, String> arguments = new HashMap<>();
    arguments.putAll(Maps.fromProperties(getConnection().getConnectionArgumentsProperties()));
    arguments.putAll(getDBSpecificArguments());
    return arguments;
  }

  @Override
  public String getJdbcPluginName() {
    return getConnection().getJdbcPluginName();
  }

  @Override
  public String getUser() {
    return getConnection().getUser();
  }

  @Override
  public String getPassword() {
    return getConnection().getPassword();
  }

  @Override
  public String getReferenceName() {
    return referenceName;
  }
}

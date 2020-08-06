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

package io.cdap.plugin.db.batch.action;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.db.ConnectionConfig;

/**
 * Config for ArgumentSetter reading from database
 */
public abstract class ArgumentSetterConfig extends ConnectionConfig {

  public static final String DATABASE_NAME = "databaseName";
  public static final String TABLE_NAME = "tableName";
  public static final String ARGUMENT_SELECTION_CONDITIONS = "argumentSelectionConditions";
  public static final String ARGUMENTS_COLUMNS = "argumentsColumns";

  @Name(ConnectionConfig.CONNECTION_STRING)
  @Description("JDBC connection string including database name.")
  @Macro
  public String connectionString;

  @Name(DATABASE_NAME)
  @Description("The name of the database which contains\n"
    + "the configuration table")
  @Macro
  String databaseName;

  @Name(TABLE_NAME)
  @Description("The name of the table in the database\n"
    + "containing the configurations for the pipeline")
  @Macro
  String tableName;

  @Name(ARGUMENT_SELECTION_CONDITIONS)
  @Description("A set of conditions for identifying the\n"
    + "arguments to run a pipeline. Users can\n"
    + "specify multiple conditions in the format\n"
    + "column1=<column1-value>;column2=<colum\n"
    + "n2-value>. A particular use case for this\n"
    + "would be feed=marketing AND\n"
    + "date=20200427. The conditions specified\n"
    + "should be logically ANDed to determine the\n"
    + "arguments for a run. When the conditions are\n"
    + "applied, the table should return exactly 1 row.\n"
    + "If it doesn’t return any rows, or if it returns\n"
    + "multiple rows, the pipeline should abort with\n"
    + "appropriate errors. Typically, users should\n"
    + "use macros in this field, so that they can\n"
    + "specify the conditions at runtime.")
  @Macro
  String argumentSelectionConditions;

  @Name(ARGUMENTS_COLUMNS)
  @Description("Names of the columns that contain the\n"
    + "arguments for this run. The values of this\n"
    + "columns in the row that satisfies the argument\n"
    + "selection conditions determines the\n"
    + "arguments for the pipeline run")
  @Macro
  String argumentsColumns;

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getArgumentSelectionConditions() {
    return argumentSelectionConditions;
  }

  public String getArgumentsColumns() {
    return argumentsColumns;
  }

  public String getQuery() {
    if (this.getArgumentSelectionConditions() == null) {
      throw new IllegalArgumentException("Argument selection conditions are empty.");
    }
    String[] split = this.getArgumentSelectionConditions().split(";");
    String conditions = String.join(" AND ", split);

    return String
      .format("SELECT %s FROM %s WHERE %s", this.getArgumentsColumns(), this.getTableName(),
              conditions);
  }

  /**
   * Validates config input fields.
   *
   * @param collector context failure collector {@link FailureCollector}
   */
  public void validate(FailureCollector collector) {
    if (!containsMacro(CONNECTION_STRING) && Strings.isNullOrEmpty(this.connectionString)) {
      collector.addFailure("Invalid connection string.", "Connection string cannot be empty.");
    }
    if (!containsMacro(ConnectionConfig.USER) && Strings.isNullOrEmpty(this.user)) {
      collector.addFailure("Invalid username.", "Username cannot be empty.");
    }
    if (!containsMacro(ConnectionConfig.PASSWORD) && Strings.isNullOrEmpty(this.password)) {
      collector.addFailure("Invalid password.", "Password cannot be empty.");
    }
    if (!containsMacro(DATABASE_NAME) && Strings.isNullOrEmpty(this.getDatabaseName())) {
      collector.addFailure("Invalid database.", "Valid database must be specified.");
    }
    if (!containsMacro(TABLE_NAME) && Strings.isNullOrEmpty(this.getTableName())) {
      collector.addFailure("Invalid table.", "Valid table must be specified.");
    }
    if (!containsMacro(ARGUMENTS_COLUMNS) && Strings.isNullOrEmpty(this.getArgumentsColumns())) {
      collector
        .addFailure("Invalid arguments columns.", "Arguments column names must be specified.");
    }
    if (!containsMacro(ARGUMENT_SELECTION_CONDITIONS) && Strings
      .isNullOrEmpty(this.getArgumentSelectionConditions())) {
      collector
        .addFailure("Invalid conditions.", "Filter conditions must be specified.");
    }
    collector.getOrThrowException();
  }
}

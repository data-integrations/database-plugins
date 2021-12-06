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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.batch.TransactionIsolationLevel;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract Config for DB Specific Source plugin
 */
public abstract class AbstractDBSpecificSourceConfig extends PluginConfig implements DatabaseSourceConfig {

  public static final String IMPORT_QUERY = "importQuery";
  public static final String BOUNDING_QUERY = "boundingQuery";
  public static final String SPLIT_BY = "splitBy";
  public static final String NUM_SPLITS = "numSplits";
  public static final String SCHEMA = "schema";
  public static final String DATABASE = "database";
  public static final String FETCH_SIZE = "fetchSize";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  public String referenceName;

  @Name(IMPORT_QUERY)
  @Description("The SELECT query to use to import data from the specified table. " +
    "You can specify an arbitrary number of columns to import, or import all columns using *. " +
    "The Query should contain the '$CONDITIONS' string unless numSplits is set to one. " +
    "For example, 'SELECT * FROM table WHERE $CONDITIONS'. The '$CONDITIONS' string" +
    "will be replaced by 'splitBy' field limits specified by the bounding query.")
  @Macro
  private String importQuery;

  @Nullable
  @Name(BOUNDING_QUERY)
  @Description("Bounding Query should return the min and max of the " +
    "values of the 'splitBy' field. For example, 'SELECT MIN(id),MAX(id) FROM table'. " +
    "This is required unless numSplits is set to one.")
  @Macro
  private String boundingQuery;

  @Nullable
  @Name(SPLIT_BY)
  @Description("Field Name which will be used to generate splits. This is required unless numSplits is set to one.")
  @Macro
  private String splitBy;

  @Nullable
  @Name(NUM_SPLITS)
  @Description("The number of splits to generate. If set to one, the boundingQuery is not needed, " +
    "and no $CONDITIONS string needs to be specified in the importQuery. If not specified, the " +
    "execution framework will pick a value.")
  @Macro
  private Integer numSplits;

  @Nullable
  @Name(SCHEMA)
  @Description("The schema of records output by the source. This will be used in place of whatever schema comes " +
    "back from the query. This should only be used if there is a bug in your jdbc driver. For example, if a column " +
    "is not correctly getting marked as nullable.")
  private String schema;

  @Nullable
  @Name(FETCH_SIZE)
  @Macro
  @Description("The number of rows to fetch at a time per split. Larger fetch size can result in faster import, " +
    "with the tradeoff of higher memory usage.")
  private Integer fetchSize;

  public String getImportQuery() {
    return cleanQuery(importQuery);
  }

  public String getBoundingQuery() {
    return cleanQuery(boundingQuery);
  }

  public void validate(FailureCollector collector) {
    boolean hasOneSplit = false;
    if (!containsMacro(NUM_SPLITS) && numSplits != null) {
      if (numSplits < 1) {
        collector.addFailure(
          String.format("Invalid value for Number of Splits '%d'. Must be at least 1.", numSplits),
          "Specify a Number of Splits no less than 1.")
          .withConfigProperty(NUM_SPLITS);
      }
      if (numSplits == 1) {
        hasOneSplit = true;
      }
    }

    if (getTransactionIsolationLevel() != null) {
      TransactionIsolationLevel.validate(getTransactionIsolationLevel(), collector);
    }

    if (!containsMacro(IMPORT_QUERY) && Strings.isNullOrEmpty(importQuery)) {
      collector.addFailure("Import Query is empty.", "Specify the Import Query.")
        .withConfigProperty(IMPORT_QUERY);
    }

    if (!hasOneSplit && !containsMacro(IMPORT_QUERY) && !getImportQuery().contains("$CONDITIONS")) {
      collector.addFailure(String.format(
        "Import Query %s must contain the string '$CONDITIONS'. if Number of Splits is not set to 1.", importQuery),
                           "Include '$CONDITIONS' in the Import Query")
        .withConfigProperty(IMPORT_QUERY);
    }

    if (!hasOneSplit && !containsMacro(SPLIT_BY) && (splitBy == null || splitBy.isEmpty())) {
      collector.addFailure("Split-By Field Name must be specified if Number of Splits is not set to 1.",
                           "Specify the Split-by Field Name.").withConfigProperty(SPLIT_BY)
        .withConfigProperty(NUM_SPLITS);
    }

    if (!hasOneSplit && !containsMacro(BOUNDING_QUERY) && (boundingQuery == null || boundingQuery.isEmpty())) {
      collector.addFailure("Bounding Query must be specified if Number of Splits is not set to 1.",
                           "Specify the Bounding Query.")
        .withConfigProperty(BOUNDING_QUERY).withConfigProperty(NUM_SPLITS);
    }

    if (!containsMacro(FETCH_SIZE) && fetchSize != null && fetchSize <= 0) {
      collector.addFailure("Invalid fetch size.", "Fetch size must be a positive integer.")
        .withConfigProperty(FETCH_SIZE);
    }
  }

  public void validateSchema(Schema actualSchema, FailureCollector collector) {
    Schema configSchema = getSchema();
    if (configSchema == null) {
      collector.addFailure("Schema should not be null or empty.", "Fill in the Schema.")
        .withConfigProperty(SCHEMA);
      return;
    }

    for (Schema.Field field : configSchema.getFields()) {
      Schema.Field actualField = actualSchema.getField(field.getName());
      if (actualField == null) {
        collector.addFailure(
          String.format("Schema field '%s' is not present in actual record", field.getName()),
          String.format("Remove the field %s in the schema.", field.getName()))
          .withOutputSchemaField(field.getName());
        continue;
      }

      Schema actualFieldSchema = actualField.getSchema().isNullable() ?
        actualField.getSchema().getNonNullable() : actualField.getSchema();
      Schema expectedFieldSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();

      validateField(collector, field, actualFieldSchema, expectedFieldSchema);
    }
  }

  protected void validateField(FailureCollector collector, Schema.Field field, Schema actualFieldSchema,
                               Schema expectedFieldSchema) {
    if (actualFieldSchema.getType() != expectedFieldSchema.getType() ||
           actualFieldSchema.getLogicalType() != expectedFieldSchema.getLogicalType()) {
      collector.addFailure(
        String.format("Schema field '%s' is expected to have type '%s but found '%s'.",
                      field.getName(), expectedFieldSchema.getDisplayName(),
                      actualFieldSchema.getDisplayName()),
        String.format("Change the data type of field %s to %s.", field.getName(), actualFieldSchema.getDisplayName()))
        .withOutputSchemaField(field.getName());
    }
  }

  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
                                                       schema, e.getMessage()), e);
    }
  }

  public String getTransactionIsolationLevel() {
    return null;
  }

  public Integer getNumSplits() {
    return numSplits;
  }

  public String getSplitBy() {
    return splitBy;
  }

  public String getConnectionString() {
    return getConnection().getConnectionString();
  }

  public Map<String, String> getConnectionArguments() {
    Map<String, String> arguments = new HashMap<>();
    arguments.putAll(Maps.fromProperties(getConnection().getConnectionArgumentsProperties()));
    arguments.putAll(getDBSpecificArguments());
    return arguments;
  }

  public String getJdbcPluginName() {
    return getConnection().getJdbcPluginName();
  }

  public String getUser() {
    return getConnection().getUser();
  }

  public String getPassword() {
    return getConnection().getPassword();
  }

  public String getReferenceName() {
    return referenceName;
  }

  public List<String> getInitQueries() {
    return Collections.emptyList();
  }

  protected String cleanQuery(@Nullable String query) {
    if (query == null) {
      return null;
    }
    return query.trim().replaceAll("[ ,]+$", "");
  }

  protected abstract Map<String, String> getDBSpecificArguments();

  protected abstract AbstractDBSpecificConnectorConfig getConnection();

  @Override
  public Integer getFetchSize() {
    return fetchSize;
  }

}

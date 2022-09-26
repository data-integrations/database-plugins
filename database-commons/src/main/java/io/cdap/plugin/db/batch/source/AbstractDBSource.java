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

package io.cdap.plugin.db.batch.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.DBConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.TransactionIsolationLevel;
import io.cdap.plugin.db.batch.config.DatabaseSourceConfig;
import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.DriverCleanup;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Batch source to read from a DB table
 * @param <T> the DB Source config
 */
public abstract class AbstractDBSource<T extends PluginConfig & DatabaseSourceConfig>
  extends ReferenceBatchSource<LongWritable, DBRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDBSource.class);
  private static final SchemaTypeAdapter SCHEMA_TYPE_ADAPTER = new SchemaTypeAdapter();
  private static final Pattern CONDITIONS_AND = Pattern.compile("\\$conditions (and|or)\\s+",
                                                                Pattern.CASE_INSENSITIVE);
  private static final Pattern AND_CONDITIONS = Pattern.compile("\\s+(and|or) \\$conditions",
                                                                Pattern.CASE_INSENSITIVE);
  private static final Pattern WHERE_CONDITIONS = Pattern.compile("\\s+where \\$conditions",
                                                                  Pattern.CASE_INSENSITIVE);

  protected final T sourceConfig;
  protected Class<? extends Driver> driverClass;

  public AbstractDBSource(T sourceConfig) {
    super(new ReferencePluginConfig(sourceConfig.getReferenceName()));
    this.sourceConfig = sourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    DBUtils.validateJDBCPluginPipeline(pipelineConfigurer, sourceConfig, getJDBCPluginId());

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    sourceConfig.validate(collector);
    if (sourceConfig.getSchema() != null) {
      stageConfigurer.setOutputSchema(sourceConfig.getSchema());
      return;
    }

    // if source config contains macro for jdbc plugin, we will not be able to access the db, just return here
    if (sourceConfig.containsMacro(ConnectionConfig.JDBC_PLUGIN_NAME)) {
      return;
    }

    Class<? extends Driver> driverClass = DBUtils.getDriverClass(
      pipelineConfigurer, sourceConfig, ConnectionConfig.JDBC_PLUGIN_TYPE);

    if (sourceConfig.canConnect()) {
      try {
        stageConfigurer.setOutputSchema(getSchema(driverClass));
      } catch (IllegalAccessException | InstantiationException e) {
        collector.addFailure("Unable to instantiate JDBC driver: " + e.getMessage(), null)
          .withStacktrace(e.getStackTrace());
      } catch (SQLException e) {
        collector.addFailure("SQL error while getting query schema: " + e.getMessage(), null)
          .withStacktrace(e.getStackTrace());
      } catch (Exception e) {
        collector.addFailure(e.getMessage(), null).withStacktrace(e.getStackTrace());
      }
    }
  }

  public Schema getSchema(Class<? extends Driver> driverClass) throws IllegalAccessException,
    SQLException, InstantiationException {
    DriverCleanup driverCleanup;
    try {

      driverCleanup = loadPluginClassAndGetDriver(driverClass);
      try (Connection connection = getConnection()) {
        executeInitQueries(connection, sourceConfig.getInitQueries());
        String query = sourceConfig.getImportQuery();
        return loadSchemaFromDB(connection, query);
      } finally {
        driverCleanup.destroy();
      }
    } catch (Exception e) {
      LOG.error("Exception while performing getSchema", e);
      throw e;
    }
  }

  private Schema loadSchemaFromDB(Connection connection, String query) throws SQLException {
    Statement statement = connection.createStatement();
    statement.setMaxRows(1);
    if (query.contains("$CONDITIONS")) {
      query = removeConditionsClause(query);
    }
    ResultSet resultSet = statement.executeQuery(query);
    return Schema.recordOf("outputSchema", getSchemaReader().getSchemaFields(resultSet));
  }

  @VisibleForTesting
  static String removeConditionsClause(String importQueryString) {
    String query = importQueryString;
    query = CONDITIONS_AND.matcher(query).replaceAll("");
    query = AND_CONDITIONS.matcher(query).replaceAll("");
    query = WHERE_CONDITIONS.matcher(query).replaceAll("");
    return query;
  }

  private Schema loadSchemaFromDB(Class<? extends Driver> driverClass)
    throws SQLException, IllegalAccessException, InstantiationException {
    String connectionString = sourceConfig.getConnectionString();
    DriverCleanup driverCleanup
      = DBUtils.ensureJDBCDriverIsAvailable(driverClass, connectionString, sourceConfig.getJdbcPluginName());

    Properties connectionProperties = new Properties();
    connectionProperties.putAll(sourceConfig.getConnectionArguments());
    try (Connection connection = DriverManager.getConnection(connectionString, connectionProperties)) {
      executeInitQueries(connection, sourceConfig.getInitQueries());
      return loadSchemaFromDB(connection, sourceConfig.getImportQuery());

    } catch (SQLException e) {
      // wrap exception to ensure SQLException-child instances not exposed to contexts without jdbc driver in classpath
      throw new SQLException(e.getMessage(), e.getSQLState(), e.getErrorCode());
    } finally {
      driverCleanup.destroy();
    }
  }

  private void executeInitQueries(Connection connection, List<String> initQueries) throws SQLException {
    for (String query : initQueries) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(query);
      }
    }
  }

  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }

  private DriverCleanup loadPluginClassAndGetDriver(Class<? extends Driver> driverClass)
    throws IllegalAccessException, InstantiationException, SQLException {

    if (driverClass == null) {
      throw new InstantiationException(
        String.format("Unable to load Driver class with plugin type %s and plugin name %s",
                      ConnectionConfig.JDBC_PLUGIN_TYPE, sourceConfig.getJdbcPluginName()));
    }

    try {
      String connectionString = createConnectionString();

      return DBUtils.ensureJDBCDriverIsAvailable(driverClass, connectionString, sourceConfig.getJdbcPluginName());
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Unable to load or register driver {}", driverClass, e);
      throw e;
    }
  }

  private Connection getConnection() throws SQLException {
    String connectionString = createConnectionString();
    Properties connectionProperties = new Properties();
    connectionProperties.putAll(sourceConfig.getConnectionArguments());
    return DriverManager.getConnection(connectionString, connectionProperties);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    sourceConfig.validate(collector);
    collector.getOrThrowException();

    String connectionString = sourceConfig.getConnectionString();

    LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
                "boundingQuery = {};",
              ConnectionConfig.JDBC_PLUGIN_TYPE, sourceConfig.getJdbcPluginName(),
              connectionString,
              sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery());
    ConnectionConfigAccessor connectionConfigAccessor = new ConnectionConfigAccessor();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    if (sourceConfig.getUser() == null && sourceConfig.getPassword() == null) {
      DBConfiguration.configureDB(connectionConfigAccessor.getConfiguration(), driverClass.getName(), connectionString);
    } else {
      DBConfiguration.configureDB(connectionConfigAccessor.getConfiguration(), driverClass.getName(), connectionString,
                                  sourceConfig.getUser(), sourceConfig.getPassword());
    }

    if (sourceConfig.getFetchSize() != null) {
      connectionConfigAccessor.setFetchSize(sourceConfig.getFetchSize());
    }

    DataDrivenETLDBInputFormat.setInput(connectionConfigAccessor.getConfiguration(), getDBRecordType(),
                                        sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery(),
                                        false);


    if (sourceConfig.getTransactionIsolationLevel() != null) {
      connectionConfigAccessor.setTransactionIsolationLevel(sourceConfig.getTransactionIsolationLevel());
    }
    connectionConfigAccessor.setConnectionArguments(sourceConfig.getConnectionArguments());
    connectionConfigAccessor.setInitQueries(sourceConfig.getInitQueries());
    if (sourceConfig.getNumSplits() == null || sourceConfig.getNumSplits() != 1) {
      if (!sourceConfig.getImportQuery().contains("$CONDITIONS")) {
        throw new IllegalArgumentException(String.format("Import Query %s must contain the string '$CONDITIONS'.",
                                                         sourceConfig.getImportQuery()));
      }
      connectionConfigAccessor.getConfiguration()
        .set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, sourceConfig.getSplitBy());
    }
    if (sourceConfig.getNumSplits() != null) {
      connectionConfigAccessor.getConfiguration().setInt(MRJobConfig.NUM_MAPS, sourceConfig.getNumSplits());
    }

    Schema schemaFromDB = loadSchemaFromDB(driverClass);
    if (sourceConfig.getSchema() != null) {
      sourceConfig.validateSchema(schemaFromDB, collector);
      collector.getOrThrowException();
      connectionConfigAccessor.setSchema(sourceConfig.getSchema().toString());
    } else {
      String schemaStr = SCHEMA_TYPE_ADAPTER.toJson(schemaFromDB);
      connectionConfigAccessor.setSchema(schemaStr);
    }

    LineageRecorder lineageRecorder = new LineageRecorder(context, sourceConfig.getReferenceName());
    Schema schema = sourceConfig.getSchema() == null ? schemaFromDB : sourceConfig.getSchema();
    lineageRecorder.createExternalDataset(schema);
    if (schema != null && schema.getFields() != null) {
      lineageRecorder.recordRead("Read", "Read from database plugin",
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
    context.setInput(Input.of(sourceConfig.getReferenceName(), new SourceInputFormatProvider(
      DataDrivenETLDBInputFormat.class, connectionConfigAccessor.getConfiguration())));
  }

  protected Class<? extends DBWritable> getDBRecordType() {
    return DBRecord.class;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
  }

  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input.getValue().getRecord());
  }

  @Override
  public void destroy() {
    DBUtils.cleanup(driverClass);
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "source", ConnectionConfig.JDBC_PLUGIN_TYPE, sourceConfig.getJdbcPluginName());
  }

  protected abstract String createConnectionString();

  /**
   * {@link PluginConfig} for {@link AbstractDBSource}
   */
  public abstract static class DBSourceConfig extends DBConfig implements DatabaseSourceConfig {
    public static final String IMPORT_QUERY = "importQuery";
    public static final String BOUNDING_QUERY = "boundingQuery";
    public static final String SPLIT_BY = "splitBy";
    public static final String NUM_SPLITS = "numSplits";
    public static final String SCHEMA = "schema";
    public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";
    public static final String FETCH_SIZE = "fetchSize";
    public static final String REFERENCE_NAME = "referenceName";

    @Name(IMPORT_QUERY)
    @Description("The SELECT query to use to import data from the specified table. " +
      "You can specify an arbitrary number of columns to import, or import all columns using *. " +
      "The Query should contain the '$CONDITIONS' string unless numSplits is set to one. " +
      "For example, 'SELECT * FROM table WHERE $CONDITIONS'. The '$CONDITIONS' string" +
      "will be replaced by 'splitBy' field limits specified by the bounding query.")
    @Macro
    public String importQuery;

    @Nullable
    @Name(BOUNDING_QUERY)
    @Description("Bounding Query should return the min and max of the " +
      "values of the 'splitBy' field. For example, 'SELECT MIN(id),MAX(id) FROM table'. " +
      "This is required unless numSplits is set to one.")
    @Macro
    public String boundingQuery;

    @Nullable
    @Name(SPLIT_BY)
    @Description("Field Name which will be used to generate splits. This is required unless numSplits is set to one.")
    @Macro
    public String splitBy;

    @Nullable
    @Name(NUM_SPLITS)
    @Description("The number of splits to generate. If set to one, the boundingQuery is not needed, " +
      "and no $CONDITIONS string needs to be specified in the importQuery. If not specified, the " +
      "execution framework will pick a value.")
    @Macro
    public Integer numSplits;

    @Nullable
    @Name(SCHEMA)
    @Description("The schema of records output by the source. This will be used in place of whatever schema comes " +
      "back from the query. This should only be used if there is a bug in your jdbc driver. For example, if a column " +
      "is not correctly getting marked as nullable.")
    public String schema;

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

    @Override
    public Integer getNumSplits() {
      return numSplits;
    }

    @Override
    public String getSplitBy() {
      return splitBy;
    }

    public void validate(FailureCollector collector) {
      boolean hasOneSplit = false;
      if (!containsMacro(NUM_SPLITS) && numSplits != null) {
        if (numSplits < 1) {
          collector.addFailure(
            String.format("Invalid value for numSplits '%d'. Must be at least 1.", numSplits), null)
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
        collector.addFailure("Import Query must be specified.", null).withConfigProperty(IMPORT_QUERY);
      }

      if (!hasOneSplit && !containsMacro(NUM_SPLITS) && !containsMacro(IMPORT_QUERY) && !getImportQuery()
        .contains("$CONDITIONS")) {
        collector.addFailure("Invalid Import Query.",
                             String.format("Import Query %s must contain the string '$CONDITIONS'.", importQuery))
          .withConfigProperty(IMPORT_QUERY);
      }

      if (!hasOneSplit && !containsMacro(NUM_SPLITS) && !containsMacro("splitBy") && (splitBy == null || splitBy
        .isEmpty())) {
        collector.addFailure("Split-By Field Name must be specified if Number of Splits is not set to 1.",
                             null).withConfigProperty(SPLIT_BY).withConfigProperty(NUM_SPLITS);
      }

      if (!hasOneSplit && !containsMacro(NUM_SPLITS) && !containsMacro(
        "boundingQuery") && (boundingQuery == null || boundingQuery.isEmpty())) {
        collector.addFailure("Bounding Query must be specified if Number of Splits is not set to 1.", null)
          .withConfigProperty(BOUNDING_QUERY).withConfigProperty(NUM_SPLITS);
      }
    }

    public void validateSchema(Schema actualSchema, FailureCollector collector) {
      validateSchema(actualSchema, getSchema(), collector);
    }

    @VisibleForTesting
    static void validateSchema(Schema actualSchema, Schema configSchema, FailureCollector collector) {
      if (configSchema == null) {
        collector.addFailure("Schema should not be null or empty.", null)
          .withConfigProperty(SCHEMA);
        return;
      }

      for (Schema.Field field : configSchema.getFields()) {
        Schema.Field actualField = actualSchema.getField(field.getName());
        if (actualField == null) {
          collector.addFailure(
            String.format("Schema field '%s' is not present in actual record", field.getName()), null)
            .withOutputSchemaField(field.getName());
          continue;
        }

        Schema actualFieldSchema = actualField.getSchema().isNullable() ?
          actualField.getSchema().getNonNullable() : actualField.getSchema();
        Schema expectedFieldSchema = field.getSchema().isNullable() ?
          field.getSchema().getNonNullable() : field.getSchema();

        if (actualFieldSchema.getType() != expectedFieldSchema.getType() ||
          actualFieldSchema.getLogicalType() != expectedFieldSchema.getLogicalType()) {
          collector.addFailure(
            String.format("Schema field '%s' has type '%s but found '%s'.",
                          field.getName(), expectedFieldSchema.getDisplayName(),
                          actualFieldSchema.getDisplayName()), null)
            .withOutputSchemaField(field.getName());
        }
      }
    }

    @Nullable
    public Schema getSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
                                                         schema, e.getMessage()), e);
      }
    }

    @Override
    public boolean canConnect() {
      return !containsMacro(ConnectionConfig.HOST) && !containsMacro(ConnectionConfig.PORT) &&
        !containsMacro(ConnectionConfig.USER) && !containsMacro(ConnectionConfig.PASSWORD) &&
        !containsMacro(DBSourceConfig.DATABASE) && !containsMacro(DBSourceConfig.IMPORT_QUERY);
    }

    @Override
    public Integer getFetchSize() {
      return fetchSize;
    }
  }

  /**
   * Request schema class.
   */
  public static class GetSchemaRequest {
    @Nullable
    public String host;
    @Nullable
    public int port;
    @Nullable
    public String database;
    @Nullable
    public String connectionString;
    @Nullable
    public String connectionArguments;
    @Nullable
    public String user;
    @Nullable
    public String password;
    public String query;
    @Nullable
    public String jdbcPluginName;
  }
}

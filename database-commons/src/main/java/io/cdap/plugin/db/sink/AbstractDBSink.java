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

package io.cdap.plugin.db.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.DBConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.config.DatabaseSinkConfig;
import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.DriverCleanup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Sink that can be configured to export data to a database table.
 * @param <T> the DB Sink config
 */
public abstract class AbstractDBSink<T extends PluginConfig & DatabaseSinkConfig>
  extends ReferenceBatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDBSink.class);

  private final T dbSinkConfig;
  private Class<? extends Driver> driverClass;
  private DriverCleanup driverCleanup;
  protected List<String> columns;
  protected List<ColumnType> columnTypes;
  protected String dbColumns;

  public AbstractDBSink(T dbSinkConfig) {
    super(new ReferencePluginConfig(dbSinkConfig.getReferenceName()));
    this.dbSinkConfig = dbSinkConfig;
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", ConnectionConfig.JDBC_PLUGIN_TYPE, dbSinkConfig.getJdbcPluginName());
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    dbSinkConfig.validate(collector);
    DBUtils.validateJDBCPluginPipeline(pipelineConfigurer, dbSinkConfig, getJDBCPluginId());
    Schema inputSchema = configurer.getInputSchema();
    if (inputSchema == null || dbSinkConfig.containsMacro(ConnectionConfig.JDBC_PLUGIN_NAME)) {
      return;
    }

    Class<? extends Driver> driverClass = DBUtils.getDriverClass(
      pipelineConfigurer, dbSinkConfig, ConnectionConfig.JDBC_PLUGIN_TYPE);
    if (driverClass != null && dbSinkConfig.canConnect()) {
      validateSchema(collector, driverClass, dbSinkConfig.getTableName(), inputSchema, dbSinkConfig.getDBSchemaName());
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    String connectionString = dbSinkConfig.getConnectionString();
    String dbSchemaName = dbSinkConfig.getDBSchemaName();
    String tableName = dbSinkConfig.getTableName();

    LOG.debug("tableName = {}; schemaName = {}, pluginType = {}; pluginName = {}; connectionString = {};",
              tableName,
              dbSchemaName,
              ConnectionConfig.JDBC_PLUGIN_TYPE,
              dbSinkConfig.getJdbcPluginName(),
              connectionString);

    Schema outputSchema = context.getInputSchema();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    // make sure that the destination table exists and column types are correct
    try {
      if (Objects.nonNull(outputSchema)) {
        FailureCollector collector = context.getFailureCollector();
        validateSchema(collector, driverClass, tableName,
                outputSchema, dbSchemaName);
        collector.getOrThrowException();
      } else {
        outputSchema = inferSchema(driverClass);
      }
    } finally {
      DBUtils.cleanup(driverClass);
    }

    setColumnsInfo(outputSchema.getFields());

    emitLineage(context, outputSchema.getFields());

    ConnectionConfigAccessor configAccessor = new ConnectionConfigAccessor();
    configAccessor.setConnectionArguments(dbSinkConfig.getConnectionArguments());
    configAccessor.setInitQueries(dbSinkConfig.getInitQueries());
    configAccessor.getConfiguration().set(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
    configAccessor.getConfiguration().set(DBConfiguration.URL_PROPERTY, connectionString);
    String fullyQualifiedTableName = dbSchemaName == null ? dbSinkConfig.getEscapedTableName()
            : dbSchemaName + "." + dbSinkConfig.getEscapedTableName();
    configAccessor.getConfiguration().set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, fullyQualifiedTableName);
    configAccessor.getConfiguration().set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, dbColumns);
    if (dbSinkConfig.getUser() != null) {
      configAccessor.getConfiguration().set(DBConfiguration.USERNAME_PROPERTY, dbSinkConfig.getUser());
    }
    if (dbSinkConfig.getPassword() != null) {
      configAccessor.getConfiguration().set(DBConfiguration.PASSWORD_PROPERTY, dbSinkConfig.getPassword());
    }

    if (!Strings.isNullOrEmpty(dbSinkConfig.getTransactionIsolationLevel())) {
      configAccessor.setTransactionIsolationLevel(dbSinkConfig.getTransactionIsolationLevel());
    }

    // Get Hadoop configuration object
    Configuration configuration = configAccessor.getConfiguration();

    // Configure batch size if specified in pipeline arguments.
    if (context.getArguments().has(ETLDBOutputFormat.COMMIT_BATCH_SIZE)) {
      configuration.set(ETLDBOutputFormat.COMMIT_BATCH_SIZE,
                        context.getArguments().get(ETLDBOutputFormat.COMMIT_BATCH_SIZE));
    }

    context.addOutput(Output.of(dbSinkConfig.getReferenceName(),
                                new SinkOutputFormatProvider(ETLDBOutputFormat.class,
                                                             configuration)));
  }

  /**
   * Extracts column info from input schema. Later it is used for metadata retrieval
   * and insert during query generation. Override this method if you need to escape column names
   * for databases with case-sensitive identifiers
   */
  protected void setColumnsInfo(List<Schema.Field> fields) {
    columns = fields.stream()
      .map(Schema.Field::getName)
      .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    dbColumns = String.join(",", columns);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
    Schema outputSchema = Optional.ofNullable(context.getInputSchema()).orElse(inferSchema(driverClass));

    setColumnsInfo(outputSchema.getFields());
    setResultSetMetadata();
  }

  private Schema inferSchema(Class<? extends Driver> driverClass) {
    List<Schema.Field> inferredFields = new ArrayList<>();
    String dbSchemaName = dbSinkConfig.getDBSchemaName();
    String fullyQualifiedTableName = dbSchemaName == null ? dbSinkConfig.getEscapedTableName()
            : dbSchemaName + "." + dbSinkConfig.getEscapedTableName();
    try {
      DBUtils.ensureJDBCDriverIsAvailable(driverClass, dbSinkConfig.getConnectionString(),
                                          dbSinkConfig.getJdbcPluginName());
      Properties connectionProperties = new Properties();
      connectionProperties.putAll(dbSinkConfig.getConnectionArguments());
      try (Connection connection = DriverManager.getConnection(dbSinkConfig.getConnectionString(),
                                                               connectionProperties)) {
        executeInitQueries(connection, dbSinkConfig.getInitQueries());

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM " + fullyQualifiedTableName
                                                     + " WHERE 1 = 0")) {
          inferredFields.addAll(getSchemaReader().getSchemaFields(rs));
        }
      } catch (SQLException e) {
        throw new InvalidStageException("Error while reading table metadata", e);

      }
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      throw new InvalidStageException("JDBC Driver unavailable: " + dbSinkConfig.getJdbcPluginName(), e);
    }
    return Schema.recordOf("inferredSchema", inferredFields);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) {
    emitter.emit(new KeyValue<>(getDBRecord(input), null));
  }

  protected DBRecord getDBRecord(StructuredRecord output) {
    return new DBRecord(output, columnTypes);
  }

  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }

  @Override
  public void destroy() {
    DBUtils.cleanup(driverClass);
    if (driverCleanup != null) {
      driverCleanup.destroy();
    }
  }

  private void setResultSetMetadata() throws Exception {
    List<ColumnType> columnTypes = new ArrayList<>(columns.size());
    String connectionString = dbSinkConfig.getConnectionString();
    String dbSchemaName = dbSinkConfig.getDBSchemaName();
    String fullyQualifiedTableName = dbSchemaName == null ? dbSinkConfig.getEscapedTableName()
            : dbSchemaName + "." + dbSinkConfig.getEscapedTableName();

    driverCleanup = DBUtils
      .ensureJDBCDriverIsAvailable(driverClass, connectionString, dbSinkConfig.getJdbcPluginName());

    Properties connectionProperties = new Properties();
    connectionProperties.putAll(dbSinkConfig.getConnectionArguments());
    try (Connection connection = DriverManager.getConnection(connectionString, connectionProperties)) {
      executeInitQueries(connection, dbSinkConfig.getInitQueries());
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               dbColumns, fullyQualifiedTableName))
      ) {
        columnTypes.addAll(getMatchedColumnTypeList(rs, columns));
      }
    }

    this.columnTypes = Collections.unmodifiableList(columnTypes);
  }

  /**
   * Compare columns from schema with columns in table and returns list of matched columns in {@link ColumnType} format.
   *
   * @param resultSet result set from table.
   * @param columns           list of columns from schema.
   * @return list of matched columns.
   */
  static List<ColumnType> getMatchedColumnTypeList(ResultSet resultSet, List<String> columns)
    throws SQLException {
    List<ColumnType> columnTypes = new ArrayList<>(columns.size());
    ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
    Map<String, String> resultSetColumnNames = new HashMap<>(resultSetMetadata.getColumnCount());

    // Populate the ResultSet field names in lower case vs original names
    // JDBC driver column indices start with index 1
    for (int i = 1; i <= resultSetMetadata.getColumnCount(); i++) {
      resultSetColumnNames.put(resultSetMetadata.getColumnName(i).toLowerCase(), resultSetMetadata.getColumnName(i));
    }

    // Iterate of all the columns present in the output schema and
    // check if the resultSet contains a column with the same name.
    for (int i = 0; i < columns.size(); i++) {
      String schemaColumnName = columns.get(i);
      String schemaColName = schemaColumnName.toLowerCase();
      Preconditions.checkArgument(resultSetColumnNames.keySet().contains(schemaColName),
              "Missing column '%s' in SQL table", schemaColumnName);

      // Find the column in the resultSet, as the index in the schema might not match with the resultSet.
      int columnIndex = resultSet.findColumn(resultSetColumnNames.get(schemaColName));
      String name = resultSetMetadata.getColumnName(columnIndex);
      String columnTypeName = resultSetMetadata.getColumnTypeName(columnIndex);
      int type = resultSetMetadata.getColumnType(columnIndex);
      columnTypes.add(new ColumnType(name, columnTypeName, type));
    }
    return columnTypes;
  }

  private void validateSchema(FailureCollector collector, Class<? extends Driver> jdbcDriverClass, String tableName,
                              Schema inputSchema, String dbSchemaName) {
    String connectionString = dbSinkConfig.getConnectionString();
    String fullyQualifiedTableName = dbSchemaName == null ? dbSinkConfig.getEscapedTableName()
            : dbSchemaName + "." + dbSinkConfig.getEscapedTableName();
    try {
      DBUtils.ensureJDBCDriverIsAvailable(jdbcDriverClass, connectionString, dbSinkConfig.getJdbcPluginName());
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      collector.addFailure(String.format("Unable to load or register JDBC driver '%s' while checking for " +
                                           "the existence of the database table '%s'.",
                                         jdbcDriverClass, fullyQualifiedTableName),
              null).withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }

    Properties connectionProperties = new Properties();
    connectionProperties.putAll(dbSinkConfig.getConnectionArguments());
    try (Connection connection = DriverManager.getConnection(connectionString, connectionProperties)) {
      executeInitQueries(connection, dbSinkConfig.getInitQueries());
      try (ResultSet tables = connection.getMetaData().getTables(null, dbSchemaName, tableName, null)) {
        if (!tables.next()) {
          collector.addFailure(
            String.format("Table '%s' does not exist.", tableName),
                          String.format("Ensure table '%s' is set correctly and that the connection string '%s' " +
                                  "points to a valid database.", fullyQualifiedTableName, connectionString))
            .withConfigProperty(DBSinkConfig.TABLE_NAME);
          return;
        }
      }
      setColumnsInfo(inputSchema.getFields());
      try (PreparedStatement pStmt = connection.prepareStatement(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                                               dbColumns,
                                                                               fullyQualifiedTableName));
           ResultSet rs = pStmt.executeQuery()) {
        getFieldsValidator().validateFields(inputSchema, rs, collector);
      }
    } catch (SQLException e) {
      LOG.error("Exception while trying to validate schema of database table {} for connection {}.",
              fullyQualifiedTableName, connectionString, e);
      collector.addFailure(
        String.format("Exception while trying to validate schema of database table '%s' for connection '%s' with %s",
                fullyQualifiedTableName, connectionString, e.getMessage()),
        null).withStacktrace(e.getStackTrace());
    }
  }

  protected FieldsValidator getFieldsValidator() {
    return new CommonFieldsValidator();
  }

  private void emitLineage(BatchSinkContext context, List<Schema.Field> fields) {
    LineageRecorder lineageRecorder = getLineageRecorder(context);

    if (!fields.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to DB table.",
                                  fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    return new LineageRecorder(context, dbSinkConfig.getReferenceName());
  }

  private void executeInitQueries(Connection connection, List<String> initQueries) throws SQLException {
    for (String query : initQueries) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(query);
      }
    }
  }

  /**
   * {@link PluginConfig} for {@link AbstractDBSink}
   */
  public abstract static class DBSinkConfig extends DBConfig implements DatabaseSinkConfig {
    public static final String TABLE_NAME = "tableName";
    public static final String DB_SCHEMA_NAME = "dbSchemaName";
    public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";

    @Name(TABLE_NAME)
    @Description("Name of the database table to write to.")
    @Macro
    public String tableName;

    @Name(DB_SCHEMA_NAME)
    @Description("Name of the database schema of table.")
    @Macro
    @Nullable
    private String dbSchemaName;

    public String getTableName() {
      return tableName;
    }

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
    public String getEscapedTableName() {
      return tableName;
    }

    public boolean canConnect() {
      return (!containsMacro(ConnectionConfig.HOST) && !containsMacro(ConnectionConfig.PORT) &&
        !containsMacro(ConnectionConfig.DATABASE) && !containsMacro(TABLE_NAME) && !containsMacro(USER) &&
        !containsMacro(PASSWORD));
    }
  }
}

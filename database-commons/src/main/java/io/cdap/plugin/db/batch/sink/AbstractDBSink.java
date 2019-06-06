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

package io.cdap.plugin.db.batch.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.db.CommonSchemaReader;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.DBConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.DriverCleanup;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Sink that can be configured to export data to a database table.
 */
public abstract class AbstractDBSink extends ReferenceBatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDBSink.class);

  private final DBSinkConfig dbSinkConfig;
  private Class<? extends Driver> driverClass;
  private DriverCleanup driverCleanup;
  protected int[] columnTypes;
  protected List<String> columns;
  protected String dbColumns;
  private Schema outputSchema;

  public AbstractDBSink(DBSinkConfig dbSinkConfig) {
    super(new ReferencePluginConfig(dbSinkConfig.referenceName));
    this.dbSinkConfig = dbSinkConfig;
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", ConnectionConfig.JDBC_PLUGIN_TYPE, dbSinkConfig.jdbcPluginName);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    DBUtils.validateJDBCPluginPipeline(pipelineConfigurer, dbSinkConfig, getJDBCPluginId());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    String connectionString = dbSinkConfig.getConnectionString();

    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {};",
              dbSinkConfig.tableName,
              ConnectionConfig.JDBC_PLUGIN_TYPE,
              dbSinkConfig.jdbcPluginName,
              connectionString);

    outputSchema = context.getInputSchema();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    // make sure that the destination table exists and column types are correct
    try {
      if (Objects.nonNull(context.getInputSchema())) {
        validateSchema(driverClass, dbSinkConfig.tableName, outputSchema);
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
    configAccessor.getConfiguration().set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY,
                                          dbSinkConfig.getEscapedTableName());
    configAccessor.getConfiguration().set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, dbColumns);
    if (dbSinkConfig.user != null) {
      configAccessor.getConfiguration().set(DBConfiguration.USERNAME_PROPERTY, dbSinkConfig.user);
    }
    if (dbSinkConfig.password != null) {
      configAccessor.getConfiguration().set(DBConfiguration.PASSWORD_PROPERTY, dbSinkConfig.password);
    }

    if (!Strings.isNullOrEmpty(dbSinkConfig.getTransactionIsolationLevel())) {
      configAccessor.setTransactionIsolationLevel(dbSinkConfig.getTransactionIsolationLevel());
    }

    context.addOutput(Output.of(dbSinkConfig.referenceName, new SinkOutputFormatProvider(ETLDBOutputFormat.class,
      configAccessor.getConfiguration())));
  }

  /**
   * Extracts column info from input schema. Later it is used for metadata retrieval
   * and insert during query generation. Override this method if you need to escape column names
   * for databases with case-sensitive identifiers
   *
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
    outputSchema = Optional.ofNullable(context.getInputSchema()).orElse(inferSchema(driverClass));

    setColumnsInfo(outputSchema.getFields());
    setResultSetMetadata();
  }

  private Schema inferSchema(Class<? extends Driver> driverClass) {
    List<Schema.Field> inferredFields = new ArrayList<>();
    try {
      DBUtils.ensureJDBCDriverIsAvailable(driverClass, dbSinkConfig.getConnectionString(), dbSinkConfig.jdbcPluginName);
      Properties connectionProperties = new Properties();
      connectionProperties.putAll(dbSinkConfig.getConnectionArguments());
      try (Connection connection = DriverManager.getConnection(dbSinkConfig.getConnectionString(),
                                                               connectionProperties)) {
        executeInitQueries(connection, dbSinkConfig.getInitQueries());

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM " + dbSinkConfig.getEscapedTableName()
                                                     + " WHERE 1 = 0")) {
          inferredFields.addAll(getSchemaReader().getSchemaFields(rs));
        }
      } catch (SQLException e) {
        throw new InvalidStageException("Error while reading table metadata", e);

      }
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      throw new InvalidStageException("JDBC Driver unavailable: " + dbSinkConfig.jdbcPluginName, e);
    }
    return Schema.recordOf("inferredSchema", inferredFields);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) {
    // Create StructuredRecord that only has the columns in this.columns
    List<Schema.Field> outputFields = new ArrayList<>();
    for (Schema.Field field : input.getSchema().getFields()) {
      Preconditions.checkArgument(columns.contains(field.getName()), "Input field '%s' is not found in columns",
                                  field.getName());
      outputFields.add(field);
    }
    StructuredRecord.Builder output = StructuredRecord.builder(outputSchema);
    for (String column : columns) {
      output.set(column, input.get(column));
    }

    emitter.emit(new KeyValue<>(getDBRecord(output), null));
  }

  protected DBRecord getDBRecord(StructuredRecord.Builder output) {
    return new DBRecord(output.build(), columnTypes);
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

  @VisibleForTesting
  public void setColumns(List<String> columns) {
    this.columns = ImmutableList.copyOf(columns);
  }

  private void setResultSetMetadata() throws Exception {
    Map<String, Integer> columnToType = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    String connectionString = dbSinkConfig.getConnectionString();

    driverCleanup = DBUtils.ensureJDBCDriverIsAvailable(driverClass, connectionString, dbSinkConfig.jdbcPluginName);

    Properties connectionProperties = new Properties();
    connectionProperties.putAll(dbSinkConfig.getConnectionArguments());
    try (Connection connection = DriverManager.getConnection(connectionString, connectionProperties)) {
      executeInitQueries(connection, dbSinkConfig.getInitQueries());
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               dbColumns,
                                                               dbSinkConfig.getEscapedTableName()))
      ) {
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        // JDBC driver column indices start with 1
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
          String name = resultSetMetadata.getColumnName(i + 1);
          int type = resultSetMetadata.getColumnType(i + 1);
          columnToType.put(name, type);
        }
      }
    }

    columnTypes = new int[columns.size()];
    for (int i = 0; i < columnTypes.length; i++) {
      String name = columns.get(i);
      Preconditions.checkArgument(columnToType.containsKey(name), "Missing column '%s' in SQL table", name);
      columnTypes[i] = columnToType.get(name);
    }
  }

  private void validateSchema(Class<? extends Driver> jdbcDriverClass, String tableName, Schema inputSchema) {
    String connectionString = dbSinkConfig.getConnectionString();

    try {
      DBUtils.ensureJDBCDriverIsAvailable(jdbcDriverClass, connectionString, dbSinkConfig.jdbcPluginName);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Unable to load or register JDBC driver {} while checking for the existence of the database table {}.",
                jdbcDriverClass, tableName, e);
      throw Throwables.propagate(e);
    }

    Properties connectionProperties = new Properties();
    connectionProperties.putAll(dbSinkConfig.getConnectionArguments());
    try (Connection connection = DriverManager.getConnection(connectionString, connectionProperties)) {
      executeInitQueries(connection, dbSinkConfig.getInitQueries());
      try (ResultSet tables = connection.getMetaData().getTables(null, null, tableName, null)) {
        if (!tables.next()) {
          throw new InvalidStageException("Table " + tableName + " does not exist. " +
                                            "Please check that the 'tableName' property has been set correctly, " +
                                            "and that the connection string  " + connectionString +
                                            "points to a valid database.");
        }
      }

      try (PreparedStatement pStmt = connection.prepareStatement("SELECT * FROM " + dbSinkConfig.getEscapedTableName()
                                                                   + " WHERE 1 = 0");
           ResultSet rs = pStmt.executeQuery()) {
        validateFields(inputSchema, rs);
      }

    } catch (SQLException e) {
      LOG.error("Exception while trying to validate schema of database table {} for connection {}.",
                tableName, connectionString, e);
      throw Throwables.propagate(e);
    }
  }

  private void validateFields(Schema inputSchema, ResultSet rs) throws SQLException {
    ResultSetMetaData rsMetaData = rs.getMetaData();

    Preconditions.checkNotNull(inputSchema.getFields());
    Set<String> invalidFields = new HashSet<>();

    for (Schema.Field field : inputSchema.getFields()) {
      int columnIndex = rs.findColumn(field.getName());
      int type = rsMetaData.getColumnType(columnIndex);
      int precision = rsMetaData.getPrecision(columnIndex);
      int scale = rsMetaData.getScale(columnIndex);

      Schema columnSchema = DBUtils.getSchema(type, precision, scale);
      boolean isColumnNullable = (ResultSetMetaData.columnNullable == rsMetaData.isNullable(columnIndex));

      Schema.Type columnType = columnSchema.getType();
      Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
        : field.getSchema().getType();
      boolean isNotNullAssignable = !isColumnNullable && field.getSchema().isNullable();
      boolean isNotCompatible = !Objects.equals(fieldType, columnType);

      if (isNotCompatible) {
        invalidFields.add(field.getName());
        LOG.error("Field {} was given as type {} but the database column is actually of type {}.", field.getName(),
                  fieldType, columnSchema.getType());
      }
      if (isNotNullAssignable) {
        invalidFields.add(field.getName());
        LOG.error("Field {} was given as nullable but the database column is not nullable", field.getName());
      }
    }

    Preconditions.checkArgument(invalidFields.isEmpty(),
                                "Couldn't find matching database column(s) for input field(s) %s.",
                                String.join(",", invalidFields));
  }

  private void emitLineage(BatchSinkContext context, List<Schema.Field> fields) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, dbSinkConfig.referenceName);

    if (!fields.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to DB table.",
                                  fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
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
  public abstract static class DBSinkConfig extends DBConfig {
    public static final String TABLE_NAME = "tableName";
    public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";

    @Name(TABLE_NAME)
    @Description("Name of the database table to write to.")
    @Macro
    public String tableName;

    /**
     * Adds escape characters (back quotes, double quotes, etc.) to the table name for
     * databases with case-sensitive identifiers.
     * @return tableName with leading and trailing escape characters appended.
     * Default implementation returns unchanged table name string.
     */
    protected String getEscapedTableName() {
      return tableName;
    }
  }
}

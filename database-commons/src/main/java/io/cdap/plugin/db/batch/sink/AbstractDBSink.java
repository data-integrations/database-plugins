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
import io.cdap.plugin.db.ColumnType;
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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sink that can be configured to export data to a database table.
 */
public abstract class AbstractDBSink extends ReferenceBatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDBSink.class);

  private final DBSinkConfig dbSinkConfig;
  private Class<? extends Driver> driverClass;
  private DriverCleanup driverCleanup;
  protected List<String> columns;
  protected List<ColumnType> columnTypes;
  protected String dbColumns;

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
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (Objects.nonNull(inputSchema)) {
      Class<? extends Driver> driverClass = DBUtils.getDriverClass(
        pipelineConfigurer, dbSinkConfig, ConnectionConfig.JDBC_PLUGIN_TYPE);
      validateSchema(driverClass, dbSinkConfig.tableName, inputSchema);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    String connectionString = dbSinkConfig.getConnectionString();

    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {};",
              dbSinkConfig.tableName,
              ConnectionConfig.JDBC_PLUGIN_TYPE,
              dbSinkConfig.jdbcPluginName,
              connectionString);

    Schema outputSchema = context.getInputSchema();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    // make sure that the destination table exists and column types are correct
    try {
      if (Objects.nonNull(outputSchema)) {
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
    Schema outputSchema = Optional.ofNullable(context.getInputSchema()).orElse(inferSchema(driverClass));

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

  @VisibleForTesting
  public void setColumns(List<String> columns) {
    this.columns = ImmutableList.copyOf(columns);
  }

  private void setResultSetMetadata() throws Exception {
    List<ColumnType> columnTypes = new ArrayList<>(columns.size());
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
          String columnTypeName = resultSetMetadata.getColumnTypeName(i + 1);
          int type = resultSetMetadata.getColumnType(i + 1);
          String schemaColumnName = columns.get(i);
          Preconditions.checkArgument(schemaColumnName.toLowerCase().equals(name.toLowerCase()),
                                      "Missing column '%s' in SQL table", schemaColumnName);
          columnTypes.add(new ColumnType(schemaColumnName, columnTypeName, type));
        }
      }
    }

    this.columnTypes = Collections.unmodifiableList(columnTypes);
  }

  private void validateSchema(Class<? extends Driver> jdbcDriverClass, String tableName, Schema inputSchema) {
    String connectionString = dbSinkConfig.getConnectionString();

    try {
      DBUtils.ensureJDBCDriverIsAvailable(jdbcDriverClass, connectionString, dbSinkConfig.jdbcPluginName);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      throw new InvalidStageException(String.format("Unable to load or register JDBC driver '%s' while checking for " +
                                                      "the existence of the database table '%s'.",
                                                    jdbcDriverClass, tableName), e);
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
      boolean isColumnNullable = (ResultSetMetaData.columnNullable == rsMetaData.isNullable(columnIndex));
      boolean isNotNullAssignable = !isColumnNullable && field.getSchema().isNullable();
      if (isNotNullAssignable) {
        LOG.error("Field '{}' was given as nullable but the database column is not nullable", field.getName());
        invalidFields.add(field.getName());
      }

      if (!isFieldCompatible(field, rsMetaData, columnIndex)) {
        String sqlTypeName = rsMetaData.getColumnTypeName(columnIndex);
        Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
        Schema.Type fieldType = fieldSchema.getType();
        Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();
        LOG.error("Field '{}' was given as type '{}' but the database column is actually of type '{}'.",
                  field.getName(),
                  fieldLogicalType != null ? fieldLogicalType.getToken() : fieldType,
                  sqlTypeName
        );
        invalidFields.add(field.getName());
      }
    }

    Preconditions.checkArgument(invalidFields.isEmpty(),
                                "Couldn't find matching database column(s) for input field(s) '%s'.",
                                String.join(",", invalidFields));
  }

  /**
   * Checks if field is compatible to be written into database column of the given sql index.
   * @param field field of the explicit input schema.
   * @param metadata resultSet metadata.
   * @param index sql column index.
   * @return 'true' if field is compatible to be written, 'false' otherwise.
   */
  protected boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    int sqlType = metadata.getColumnType(index);

    return isFieldCompatible(fieldType, fieldLogicalType, sqlType);
  }

  protected boolean isFieldCompatible(Schema.Type fieldType, Schema.LogicalType fieldLogicalType, int sqlType) {
    // Handle logical types first
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DATE:
          return sqlType == Types.DATE;
        case TIME_MILLIS:
        case TIME_MICROS:
          return sqlType == Types.TIME;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return sqlType == Types.TIMESTAMP;
        case DECIMAL:
          return sqlType == Types.NUMERIC
            || sqlType == Types.DECIMAL;
      }
    }

    switch (fieldType) {
      case NULL:
        return true;
      case BOOLEAN:
        return sqlType == Types.BOOLEAN
          || sqlType == Types.BIT;
      case INT:
        return sqlType == Types.INTEGER
          || sqlType == Types.SMALLINT
          || sqlType == Types.TINYINT;
      case LONG:
        return sqlType == Types.BIGINT;
      case FLOAT:
        return sqlType == Types.REAL
          || sqlType == Types.FLOAT;
      case DOUBLE:
        return sqlType == Types.DOUBLE;
      case BYTES:
        return sqlType == Types.BINARY
          || sqlType == Types.VARBINARY
          || sqlType == Types.LONGVARBINARY
          || sqlType == Types.BLOB;
      case STRING:
        return sqlType == Types.VARCHAR
          || sqlType == Types.CHAR
          || sqlType == Types.CLOB
          || sqlType == Types.LONGNVARCHAR
          || sqlType == Types.LONGVARCHAR
          || sqlType == Types.NCHAR
          || sqlType == Types.NCLOB
          || sqlType == Types.NVARCHAR;
      default:
        return false;
    }
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

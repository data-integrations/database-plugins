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

package co.cask.db.batch.sink;

import co.cask.ConnectionConfig;
import co.cask.DBConfig;
import co.cask.DBRecord;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.db.batch.TransactionIsolationLevel;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.util.DBUtils;
import co.cask.util.DriverCleanup;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
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
  private String dbColumns;

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

    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {}; columns = {};",
              dbSinkConfig.tableName,
              ConnectionConfig.JDBC_PLUGIN_TYPE,
              dbSinkConfig.jdbcPluginName,
              connectionString, "columns");

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    // make sure that the table exists
    try {
      Preconditions.checkArgument(
        tableExists(driverClass, dbSinkConfig.tableName),
        "Table %s does not exist. Please check that the 'tableName' property " +
          "has been set correctly, and that the connection string %s points to a valid database.",
        dbSinkConfig.tableName, connectionString);
    } finally {
      DBUtils.cleanup(driverClass);
    }

    Schema inputSchema = context.getInputSchema();
    Objects.requireNonNull(inputSchema, "AbstractDBSink requires a single known schema.");

    setColumnsInfo(inputSchema.getFields());

    emitLineage(context, inputSchema.getFields());

    context.addOutput(Output.of(dbSinkConfig.referenceName,
                                new DBOutputFormatProvider(dbSinkConfig, connectionString, dbColumns, driverClass)));
  }

  private void setColumnsInfo(List<Schema.Field> fields) {
    columns = fields.stream()
      .map(Schema.Field::getName)
      .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    dbColumns = String.join(",", columns);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
    setColumnsInfo(context.getInputSchema().getFields());
    setResultSetMetadata();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) throws Exception {
    // Create StructuredRecord that only has the columns in this.columns
    List<Schema.Field> outputFields = new ArrayList<>();
    for (String column : columns) {
      Schema.Field field = input.getSchema().getField(column);
      Preconditions.checkNotNull(field, "Column '%s' not found in an input record", column);
      outputFields.add(field);
    }
    StructuredRecord.Builder output = StructuredRecord.builder(
      Schema.recordOf(input.getSchema().getRecordName(), outputFields));
    for (String column : columns) {
      output.set(column, input.get(column));
    }

    emitter.emit(new KeyValue<>(getDBRecord(output), null));
  }

  protected DBRecord getDBRecord(StructuredRecord.Builder output) {
    return new DBRecord(output.build(), columnTypes);
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

    try (Connection connection = DriverManager.getConnection(connectionString,
                                                             dbSinkConfig.getConnectionArguments())) {
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               dbColumns,
                                                               dbSinkConfig.tableName))
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

  private boolean tableExists(Class<? extends Driver> jdbcDriverClass, String tableName) {
    String connectionString = dbSinkConfig.getConnectionString();

    try {
      DBUtils.ensureJDBCDriverIsAvailable(jdbcDriverClass, connectionString, dbSinkConfig.jdbcPluginName);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Unable to load or register JDBC driver {} while checking for the existence of the database table {}.",
                jdbcDriverClass, tableName, e);
      throw Throwables.propagate(e);
    }

    try (Connection connection = DriverManager.getConnection(connectionString, dbSinkConfig.getConnectionArguments())) {
      DatabaseMetaData metadata = connection.getMetaData();
      try (ResultSet rs = metadata.getTables(null, null, tableName, null)) {
        return rs.next();
      }
    } catch (SQLException e) {
      LOG.error("Exception while trying to check the existence of database table {} for connection {}.",
                tableName, connectionString, e);
      throw Throwables.propagate(e);
    }
  }

  private void emitLineage(BatchSinkContext context, List<Schema.Field> fields) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, dbSinkConfig.referenceName);

    if (!fields.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to DB table.",
                                  fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  /**
   * {@link PluginConfig} for {@link AbstractDBSink}
   */
  public abstract static class DBSinkConfig extends DBConfig {
    public static final String COLUMNS = "columns";
    public static final String TABLE_NAME = "tableName";
    public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";

    @Name(TABLE_NAME)
    @Description("Name of the database table to write to.")
    @Macro
    public String tableName;
  }

  private static class DBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    DBOutputFormatProvider(DBSinkConfig dbSinkConfig, String connectionString,
                           String dbColumns, Class<? extends Driver> driverClass) {
      this.conf = new HashMap<>();

      if (dbSinkConfig.getTransactionIsolationLevel() != null) {
        conf.put(TransactionIsolationLevel.CONF_KEY,
                 dbSinkConfig.getTransactionIsolationLevel());
      }
      if (dbSinkConfig.connectionArguments != null) {
        conf.put(DBUtils.CONNECTION_ARGUMENTS, dbSinkConfig.connectionArguments);
      }
      conf.put(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
      conf.put(DBConfiguration.URL_PROPERTY, connectionString);
      if (dbSinkConfig.user != null) {
        conf.put(DBConfiguration.USERNAME_PROPERTY, dbSinkConfig.user);
      }
      if (dbSinkConfig.password != null) {
        conf.put(DBConfiguration.PASSWORD_PROPERTY, dbSinkConfig.password);
      }
      conf.put(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, dbSinkConfig.tableName);
      conf.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, dbColumns);
    }

    @Override
    public String getOutputFormatClassName() {
      return ETLDBOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}

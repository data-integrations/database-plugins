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

package co.cask.db.batch.source;

import co.cask.CommonSchemaReader;
import co.cask.ConnectionConfig;
import co.cask.DBConfig;
import co.cask.DBRecord;
import co.cask.FieldCase;
import co.cask.SchemaReader;
import co.cask.StructuredRecordUtils;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.db.batch.TransactionIsolationLevel;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.util.DBUtils;
import co.cask.util.DriverCleanup;
import com.google.common.base.Strings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Batch source to read from a DB table
 */
public abstract class AbstractDBSource extends ReferenceBatchSource<LongWritable, DBRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDBSource.class);

  protected final DBSourceConfig sourceConfig;
  protected Class<? extends Driver> driverClass;
  protected FieldCase fieldCase;

  public AbstractDBSource(DBSourceConfig sourceConfig) {
    super(new ReferencePluginConfig(sourceConfig.referenceName));
    this.sourceConfig = sourceConfig;
  }

  private static String removeConditionsClause(String importQueryString) {
    importQueryString = importQueryString.replaceAll("\\s{2,}", " ").toUpperCase();
    if (importQueryString.contains("WHERE $CONDITIONS AND")) {
      importQueryString = importQueryString.replace("$CONDITIONS AND", "");
    } else if (importQueryString.contains("WHERE $CONDITIONS")) {
      importQueryString = importQueryString.replace("WHERE $CONDITIONS", "");
    } else if (importQueryString.contains("AND $CONDITIONS")) {
      importQueryString = importQueryString.replace("AND $CONDITIONS", "");
    } else if (importQueryString.contains("$CONDITIONS")) {
      throw new IllegalArgumentException("Please remove the $CONDITIONS clause when fetching the input schema.");
    }
    return importQueryString;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    DBUtils.validateJDBCPluginPipeline(pipelineConfigurer, sourceConfig, getJDBCPluginId());
    sourceConfig.validate();
    if (!Strings.isNullOrEmpty(sourceConfig.schema)) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(sourceConfig.getSchema());
    }
  }

  /**
   * Endpoint method to get the output schema of a query.
   *
   * @param request       {@link GetSchemaRequest} containing information required for connection and query to execute.
   * @param pluginContext context to create plugins
   * @return schema of fields
   * @throws SQLException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  @Path("getSchema")
  public Schema getSchema(GetSchemaRequest request,
                          EndpointPluginContext pluginContext) throws IllegalAccessException,
    SQLException, InstantiationException, ClassNotFoundException {
    DriverCleanup driverCleanup;
    try {

      driverCleanup = loadPluginClassAndGetDriver(request, pluginContext);
      try (Connection connection = getConnection(request)) {
        String query = request.query;
        Statement statement = connection.createStatement();
        statement.setMaxRows(1);
        if (query.contains("$CONDITIONS")) {
          query = removeConditionsClause(query);
        }
        ResultSet resultSet = statement.executeQuery(query);
        return Schema.recordOf("outputSchema", getSchemaReader().getSchemaFields(resultSet));
      } finally {
        driverCleanup.destroy();
      }
    } catch (Exception e) {
      LOG.error("Exception while performing getSchema", e);
      throw e;
    }
  }

  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }

  private DriverCleanup loadPluginClassAndGetDriver(GetSchemaRequest request,
                                                    EndpointPluginContext pluginContext)
    throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {

    Class<? extends Driver> driverClass =
      pluginContext.loadPluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE,
                                    request.jdbcPluginName, PluginProperties.builder().build());

    if (driverClass == null) {
      throw new InstantiationException(
        String.format("Unable to load Driver class with plugin type %s and plugin name %s",
                      ConnectionConfig.JDBC_PLUGIN_TYPE, request.jdbcPluginName));
    }

    try {
      String connectionString =
        Objects.nonNull(request.connectionString) ? request.connectionString : createConnectionString(
          request.host,
          request.port,
          request.database);

      return DBUtils.ensureJDBCDriverIsAvailable(driverClass, connectionString, request.jdbcPluginName);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Unable to load or register driver {}", driverClass, e);
      throw e;
    }
  }

  private Connection getConnection(GetSchemaRequest getSchemaRequest) throws SQLException {
    Properties properties =
      ConnectionConfig.getConnectionArguments(getSchemaRequest.connectionArguments,
                                              getSchemaRequest.user,
                                              getSchemaRequest.password);

    String connectionString =
      Objects.nonNull(getSchemaRequest.connectionString) ? getSchemaRequest.connectionString : createConnectionString(
        getSchemaRequest.host,
        getSchemaRequest.port,
        getSchemaRequest.database);

    return DriverManager.getConnection(connectionString, properties);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    sourceConfig.validate();

    String connectionString = sourceConfig.getConnectionString();

    LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
                "boundingQuery = {};",
              ConnectionConfig.JDBC_PLUGIN_TYPE, sourceConfig.jdbcPluginName,
              connectionString,
              sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery());
    JobConf hConf = new JobConf();
    hConf.clear();

    int fetchSize = 1000;
    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    if (sourceConfig.user == null && sourceConfig.password == null) {
      DBConfiguration.configureDB(hConf, driverClass.getName(), connectionString, fetchSize);
    } else {
      DBConfiguration.configureDB(hConf, driverClass.getName(), connectionString,
                                  sourceConfig.user, sourceConfig.password, fetchSize);
      hConf.set("co.cask.cdap.jdbc.passwd", sourceConfig.password);
    }

    DataDrivenETLDBInputFormat.setInput(hConf, getDBRecordType(),
                                        sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery(),
                                        false);



    if (sourceConfig.getTransactionIsolationLevel() != null) {
      hConf.set(TransactionIsolationLevel.CONF_KEY,
                sourceConfig.getTransactionIsolationLevel());
    }
    if (sourceConfig.connectionArguments != null) {
      hConf.set(DBUtils.CONNECTION_ARGUMENTS, sourceConfig.connectionArguments);
    }
    if (sourceConfig.numSplits == null || sourceConfig.numSplits != 1) {
      if (!sourceConfig.getImportQuery().contains("$CONDITIONS")) {
        throw new IllegalArgumentException(String.format("Import Query %s must contain the string '$CONDITIONS'.",
                                                         sourceConfig.importQuery));
      }
      hConf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, sourceConfig.splitBy);
    }
    if (sourceConfig.numSplits != null) {
      hConf.setInt(MRJobConfig.NUM_MAPS, sourceConfig.numSplits);
    }
    if (sourceConfig.schema != null) {
      hConf.set(DBUtils.OVERRIDE_SCHEMA, sourceConfig.schema);
    }
    LineageRecorder lineageRecorder = new LineageRecorder(context, sourceConfig.referenceName);
    lineageRecorder.createExternalDataset(sourceConfig.getSchema());
    context.setInput(Input.of(sourceConfig.referenceName,
                              new SourceInputFormatProvider(DataDrivenETLDBInputFormat.class, hConf)));
  }

  protected Class<? extends org.apache.sqoop.mapreduce.DBWritable> getDBRecordType() {
    return DBRecord.class;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());;
    fieldCase = FieldCase.toFieldCase(sourceConfig.columnNameCase);
  }

  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecordUtils.convertCase(input.getValue().getRecord(), fieldCase));
  }

  @Override
  public void destroy() {
    DBUtils.cleanup(driverClass);
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "source", ConnectionConfig.JDBC_PLUGIN_TYPE, sourceConfig.jdbcPluginName);
  }

  protected abstract String createConnectionString(String host, Integer port, String database);

  /**
   * {@link PluginConfig} for {@link AbstractDBSource}
   */
  public abstract static class DBSourceConfig extends DBConfig {
    public static final String IMPORT_QUERY = "importQuery";
    public static final String BOUNDING_QUERY = "boundingQuery";
    public static final String SPLIT_BY = "splitBy";
    public static final String NUM_SPLITS = "numSplits";
    public static final String SCHEMA = "schema";
    public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";
    public static final String COLUMN_NAME_CASE = "columnCase";

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


    @Name(COLUMN_NAME_CASE)
    @Description("Sets the case of the column names returned from the query. " +
      "Possible options are upper or lower. By default or for any other input, the column names are not modified and " +
      "the names returned from the database are used as-is. Note that setting this property provides predictability " +
      "of column name cases across different databases but might result in column name conflicts if multiple column " +
      "names are the same when the case is ignored.")
    @Nullable
    public String columnNameCase;


    private String getImportQuery() {
      return cleanQuery(importQuery);
    }

    private String getBoundingQuery() {
      return cleanQuery(boundingQuery);
    }

    private void validate() {
      boolean hasOneSplit = false;
      if (!containsMacro("numSplits") && numSplits != null) {
        if (numSplits < 1) {
          throw new IllegalArgumentException(
            "Invalid value for numSplits. Must be at least 1, but got " + numSplits);
        }
        if (numSplits == 1) {
          hasOneSplit = true;
        }
      }

      if (getTransactionIsolationLevel() != null) {
        TransactionIsolationLevel.validate(getTransactionIsolationLevel());
      }

      if (!hasOneSplit && !containsMacro("importQuery") && !getImportQuery().contains("$CONDITIONS")) {
        throw new IllegalArgumentException(String.format("Import Query %s must contain the string '$CONDITIONS'.",
                                                         importQuery));
      }

      if (!hasOneSplit && !containsMacro("splitBy") && (splitBy == null || splitBy.isEmpty())) {
        throw new IllegalArgumentException("The splitBy must be specified if numSplits is not set to 1.");
      }

      if (!hasOneSplit && !containsMacro("boundingQuery") && (boundingQuery == null || boundingQuery.isEmpty())) {
        throw new IllegalArgumentException("The boundingQuery must be specified if numSplits is not set to 1.");
      }

    }

    @Nullable
    private Schema getSchema() {
      try {
        return schema == null ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
                                                         schema, e.getMessage()), e);
      }
    }
  }

  /**
   * Request schema class.
   */
  public class GetSchemaRequest {
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

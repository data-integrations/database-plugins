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

package io.cdap.plugin.neo4j.source;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.neo4j.Neo4jRecord;
import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.DriverCleanup;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Batch source to read from Neo4j.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(Neo4jSource.NAME)
@Description("Reads from a Neo4j instance using a configurable CQL query. " +
  "Outputs one record for each row returned by the query.")
public class Neo4jSource extends ReferenceBatchSource<LongWritable, Neo4jRecord, StructuredRecord> {
  public static final String NAME = "Neo4jSource";
  private static final Logger LOG = LoggerFactory.getLogger(Neo4jSource.class);

  private static final SchemaTypeAdapter SCHEMA_TYPE_ADAPTER = new SchemaTypeAdapter();

  protected Class<? extends Driver> driverClass;

  private final Neo4jSourceConfig config;

  public Neo4jSource(Neo4jSourceConfig config) {
    super(new ReferencePluginConfig(config.getReferenceName()));
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);

    Class<? extends Driver> driverClass = pipelineConfigurer.usePluginClass(
      ConnectionConfig.JDBC_PLUGIN_TYPE,
      config.getJdbcPluginName(),
      getJDBCPluginId(), PluginProperties.builder().build());
    Preconditions.checkArgument(
      driverClass != null, "Unable to load JDBC Driver class for plugin name '%s'. Please make sure " +
        "that the plugin '%s' of type '%s' containing the driver has been installed correctly.",
      config.getJdbcPluginName(),
      config.getJdbcPluginName(), ConnectionConfig.JDBC_PLUGIN_TYPE);
    try {
      Class.forName(driverClass.getName());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    ConnectionConfigAccessor connectionConfigAccessor = new ConnectionConfigAccessor();
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());

    DBConfiguration.configureDB(connectionConfigAccessor.getConfiguration(), driverClass.getName(),
                                config.getConnectionString(), config.getUsername(), config.getPassword());
    Neo4jDataDriveDBInputFormat.setInput(connectionConfigAccessor.getConfiguration(), getDBRecordType(),
                                         config.getInputQuery(), config.getOrderBy(),
                                         false);

    connectionConfigAccessor.getConfiguration().setInt(MRJobConfig.NUM_MAPS, config.getSplitNum());
    Schema schemaFromDB = loadSchemaFromDB(driverClass);
    String schemaStr = SCHEMA_TYPE_ADAPTER.toJson(schemaFromDB);
    connectionConfigAccessor.setSchema(schemaStr);

    context.setInput(Input.of(config.getReferenceName(), new SourceInputFormatProvider(
      Neo4jDataDriveDBInputFormat.class, connectionConfigAccessor.getConfiguration())));
  }

  @Override
  public void transform(KeyValue<LongWritable, Neo4jRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue().getRecord());
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "source", ConnectionConfig.JDBC_PLUGIN_TYPE, "neo4j");
  }

  public Schema getSchema() {
    try (Connection connection = this.getConnection()) {
      String query = config.getInputQuery();
      return loadSchemaFromDB(connection, query);
    } catch (Exception ex) {
      throw new IllegalStateException("Exception while performing getSchema", ex);
    }
  }

  private Schema loadSchemaFromDB(Connection connection, String query) throws SQLException {
    Statement statement = connection.createStatement();
    statement.setMaxRows(1);
    ResultSet resultSet = statement.executeQuery(query);
    return Schema.recordOf("outputSchema", this.getSchemaReader().getSchemaFields(resultSet));
  }

  private Schema loadSchemaFromDB(Class<? extends Driver> driverClass)
    throws SQLException, IllegalAccessException, InstantiationException {
    String connectionString = config.getConnectionString();
    DriverCleanup driverCleanup
      = DBUtils.ensureJDBCDriverIsAvailable(driverClass, connectionString, "neo4j");

    Properties connectionProperties = new Properties();
    try (Connection connection = DriverManager.getConnection(connectionString, connectionProperties)) {
      return loadSchemaFromDB(connection, config.getInputQuery());

    } catch (SQLException e) {
      // wrap exception to ensure SQLException-child instances not exposed to contexts without jdbc driver in classpath
      throw new SQLException(e.getMessage(), e.getSQLState(), e.getErrorCode());
    } finally {
      driverCleanup.destroy();
    }
  }

  private Connection getConnection() throws SQLException {
    String connectionString = config.getConnectionString();
    return DriverManager.getConnection(connectionString);
  }

  protected SchemaReader getSchemaReader() {
    return new Neo4jSchemaReader();
  }

  protected Class<? extends DBWritable> getDBRecordType() {
    return Neo4jRecord.class;
  }

}

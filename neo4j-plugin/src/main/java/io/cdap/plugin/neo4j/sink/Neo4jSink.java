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

package io.cdap.plugin.neo4j.sink;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.neo4j.Neo4jConstants;
import io.cdap.plugin.neo4j.Neo4jRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Batch sink to write to Neo4j.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(Neo4jSink.NAME)
@Description("Writes records to a Neo4j database.")
public class Neo4jSink extends ReferenceBatchSink<StructuredRecord, Neo4jRecord, NullWritable> {
  public static final String NAME = "Neo4jSink";
  private static final Logger LOG = LoggerFactory.getLogger(Neo4jSink.class);

  private Neo4jSinkConfig config;

  private Class<? extends Driver> driverClass;
  private List<ColumnType> columnTypes;
  protected List<String> columns;
  protected String dbColumns;

  public Neo4jSink(Neo4jSinkConfig config) {
    super(new ReferencePluginConfig(config.getReferenceName()));
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector, inputSchema);

    Class<? extends Driver> driverClass = pipelineConfigurer.usePluginClass(
      ConnectionConfig.JDBC_PLUGIN_TYPE,
      "neo4j",
      getJDBCPluginId(), PluginProperties.builder().build());
    Preconditions.checkArgument(
      driverClass != null, "Unable to load JDBC Driver class for plugin name '%s'. Please make sure " +
        "that the plugin '%s' of type '%s' containing the driver has been installed correctly.",
      "neo4j",
      "neo4j", ConnectionConfig.JDBC_PLUGIN_TYPE);
    try {
      Class.forName(driverClass.getName());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);

    driverClass = context.loadPluginClass(getJDBCPluginId());

    Schema outputSchema = Optional.ofNullable(context.getInputSchema()).orElse(null);

    if (outputSchema != null) {
      setColumnsInfo(outputSchema.getFields());
      setColumnsType(outputSchema.getFields());
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getInputSchema());
    collector.getOrThrowException();

    Schema outputSchema = context.getInputSchema();
    setColumnsInfo(outputSchema.getFields());
    emitLineage(context, outputSchema.getFields());
    driverClass = context.loadPluginClass(getJDBCPluginId());

    ConnectionConfigAccessor configAccessor = new ConnectionConfigAccessor();
    configAccessor.getConfiguration().set(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
    configAccessor.getConfiguration().set(DBConfiguration.URL_PROPERTY, config.getConnectionString());
    configAccessor.getConfiguration().set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, dbColumns);
    configAccessor.getConfiguration().set(DBConfiguration.USERNAME_PROPERTY, config.getUsername());
    configAccessor.getConfiguration().set(DBConfiguration.PASSWORD_PROPERTY, config.getPassword());
    configAccessor.getConfiguration().set(Neo4jConstants.OUTPUT_QUERY, config.getOutputQuery());

    context.addOutput(Output.of(config.getReferenceName(),
                                new SinkOutputFormatProvider(Neo4jDataDriveDBOutputFormat.class,
                                                             configAccessor.getConfiguration())));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Neo4jRecord, NullWritable>> emitter) {
    emitter.emit(new KeyValue<>(getNeo4jRecord(input), null));
  }

  private Neo4jRecord getNeo4jRecord(StructuredRecord output) {
    return new Neo4jRecord(output, columnTypes);
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", ConnectionConfig.JDBC_PLUGIN_TYPE, "neo4j");
  }

  private void setColumnsInfo(List<Schema.Field> fields) {
    columns = fields.stream()
      .map(Schema.Field::getName)
      .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    dbColumns = String.join(",", columns);
  }

  private void setColumnsType(List<Schema.Field> fields) {
    columnTypes = fields.stream().map(v -> new ColumnType(v.getName(), null, 0)).collect(Collectors.toList());
  }

  private void emitLineage(BatchSinkContext context, List<Schema.Field> fields) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());

    if (!fields.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to DB table.",
                                  fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}

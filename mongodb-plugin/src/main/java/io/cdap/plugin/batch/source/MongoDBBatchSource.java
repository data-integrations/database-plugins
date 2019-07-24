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

package io.cdap.plugin.batch.source;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.StandaloneMongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.MongoDBConfig;
import io.cdap.plugin.MongoDBConstants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads data from MongoDB and converts each document into 
 * a {@link StructuredRecord} with the help of the specified Schema.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MongoDBConstants.PLUGIN_NAME)
@Description("MongoDB Batch Source will read documents from MongoDB and convert each document " +
  "into a StructuredRecord with the help of the specified Schema. ")
public class MongoDBBatchSource extends ReferenceBatchSource<Object, BSONObject, StructuredRecord> {

  private final MongoDBSourceConfig config;
  private BSONObjectToRecordTransformer bsonObjectToRecordTransformer;

  public MongoDBBatchSource(MongoDBSourceConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    Schema schema = config.getSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    config.validate();
    Configuration conf = new Configuration();
    conf.clear();

    MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
    MongoConfigUtil.setInputURI(conf, config.getConnectionString());
    if (!Strings.isNullOrEmpty(config.inputQuery)) {
      MongoConfigUtil.setQuery(conf, config.inputQuery);
    }
    if (!Strings.isNullOrEmpty(config.authConnectionString)) {
      MongoConfigUtil.setAuthURI(conf, config.authConnectionString);
    }
    if (!Strings.isNullOrEmpty(config.inputFields)) {
      MongoConfigUtil.setFields(conf, config.inputFields);
    }
    if (!Strings.isNullOrEmpty(config.splitterClass)) {
      String className = String.format("%s.%s", StandaloneMongoSplitter.class.getPackage().getName(),
                                       config.splitterClass);
      Class<? extends MongoSplitter> klass = getClass().getClassLoader().loadClass(
        className).asSubclass(MongoSplitter.class);
      MongoConfigUtil.setSplitterClass(conf, klass);
    }

    emitLineage(context);
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(MongoConfigUtil.getInputFormat(conf), conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    bsonObjectToRecordTransformer = new BSONObjectToRecordTransformer(config.getSchema());
  }

  @Override
  public void transform(KeyValue<Object, BSONObject> input, Emitter<StructuredRecord> emitter) throws Exception {
    BSONObject bsonObject = input.getValue();
    emitter.emit(bsonObjectToRecordTransformer.transform(bsonObject));
  }

  private void emitLineage(BatchSourceContext context) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    List<Schema.Field> fields = Objects.requireNonNull(config.getSchema()).getFields();
    if (fields != null && !fields.isEmpty()) {
      lineageRecorder.recordRead("Read",
                                 String.format("Read from '%s' MongoDB collection.", config.collection),
                                 fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  /**
   * Config class for {@link MongoDBBatchSource}.
   */
  public static class MongoDBSourceConfig extends MongoDBConfig {

    public static final Set<Schema.Type> SUPPORTED_FIELD_TYPES = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.BOOLEAN,
                                                                                 Schema.Type.BYTES, Schema.Type.STRING,
                                                                                 Schema.Type.DOUBLE, Schema.Type.FLOAT,
                                                                                 Schema.Type.INT, Schema.Type.LONG);
    @Name(MongoDBConstants.SCHEMA)
    @Description("Schema of records output by the source.")
    private String schema;

    @Name(MongoDBConstants.INPUT_QUERY)
    @Description("Optionally filter the input collection with a query. This query must be represented in JSON " +
      "format, and use the MongoDB extended JSON format to represent non-native JSON data types.")
    @Nullable
    @Macro
    private String inputQuery;

    @Name(MongoDBConstants.INPUT_FIELDS)
    @Nullable
    @Description("A projection document limiting the fields that appear in each document. " +
      "If no projection document is provided, all fields will be read.")
    @Macro
    private String inputFields;

    @Name(MongoDBConstants.SPLITTER_CLASS)
    @Nullable
    @Description("The name of the Splitter class to use. If left empty, the MongoDB Hadoop Connector will attempt " +
      "to make a best guess as to what Splitter to use.")
    @Macro
    private String splitterClass;

    @Name(MongoDBConstants.AUTH_CONNECTION_STRING)
    @Nullable
    @Description("Auxiliary MongoDB connection string to authenticate against when constructing splits.")
    @Macro
    private String authConnectionString;

    /**
     * @return the schema of the dataset
     */
    @Nullable
    public Schema getSchema() {
      try {
        return null == schema ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new InvalidConfigPropertyException("Invalid schema", e, MongoDBConstants.SCHEMA);
      }
    }

    @Override
    public void validate() {
      super.validate();
      if (!containsMacro(MongoDBConstants.SCHEMA)) {
        Schema parsedSchema = getSchema();
        if (null == parsedSchema) {
          throw new InvalidConfigPropertyException("Schema must be specified", MongoDBConstants.SCHEMA);
        }
        List<Schema.Field> fields = parsedSchema.getFields();
        if (null == fields || fields.isEmpty()) {
          throw new InvalidConfigPropertyException("Schema should contain fields to map", MongoDBConstants.SCHEMA);
        }
        for (Schema.Field field : fields) {
          Schema.Type fieldType = field.getSchema().isNullable()
            ? field.getSchema().getNonNullable().getType()
            : field.getSchema().getType();
          if (!SUPPORTED_FIELD_TYPES.contains(fieldType)) {
            String supportedTypes = SUPPORTED_FIELD_TYPES.stream()
              .map(Enum::name)
              .map(String::toLowerCase)
              .collect(Collectors.joining(", "));
            String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s. ",
                                                field.getName(), fieldType, supportedTypes);
            throw new InvalidConfigPropertyException(errorMessage, MongoDBConstants.SCHEMA);
          }
        }
      }
    }
  }
}

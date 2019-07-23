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

package io.cdap.plugin.batch.sink;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.MongoDBConfig;
import io.cdap.plugin.MongoDBConstants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A {@link BatchSink} that writes data to MongoDB.
 * This {@link MongoDBBatchSink} takes a {@link StructuredRecord} in,
 * converts it to {@link BSONWritable}, and writes it to MongoDB.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MongoDBConstants.PLUGIN_NAME)
@Description("MongoDB Batch Sink converts a StructuredRecord to a BSONWritable and writes it to MongoDB.")
public class MongoDBBatchSink extends ReferenceBatchSink<StructuredRecord, NullWritable, BSONWritable> {

  private final MongoDBConfig config;

  public MongoDBBatchSink(MongoDBConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Configuration conf = new Configuration();
    String path = conf.get(
      "mapreduce.task.tmp.dir",
      conf.get(
        "mapred.child.tmp",
        conf.get("hadoop.tmp.dir", System.getProperty("java.io.tmpdir")))) + "/" + UUID.randomUUID().toString();

    if (Objects.nonNull(context.getInputSchema())) {
      LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
      lineageRecorder.createExternalDataset(context.getInputSchema());
      List<Schema.Field> fields = context.getInputSchema().getFields();
      if (fields != null && !fields.isEmpty()) {
        lineageRecorder.recordWrite("Write", "Wrote to MongoDB collection.",
                                    fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
      }
    }

    context.addOutput(Output.of(config.referenceName, new MongoDBOutputFormatProvider(config, path)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, BSONWritable>> emitter)
    throws Exception {
    BasicDBObjectBuilder bsonBuilder = BasicDBObjectBuilder.start();
    for (Schema.Field field : input.getSchema().getFields()) {
      bsonBuilder.add(field.getName(), input.get(field.getName()));
    }
    emitter.emit(new KeyValue<>(NullWritable.get(), new BSONWritable(bsonBuilder.get())));
  }

  private static class MongoDBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    MongoDBOutputFormatProvider(MongoDBConfig config, String path) {
      this.conf = new HashMap<>();
      conf.put("mongo.output.uri", config.getConnectionString());
      conf.put("mapreduce.task.tmp.dir", path);
    }

    @Override
    public String getOutputFormatClassName() {
      return MongoOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}

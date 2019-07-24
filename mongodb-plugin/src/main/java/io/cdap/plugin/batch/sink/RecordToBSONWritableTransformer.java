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
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.List;
import java.util.Objects;

/**
 * Transforms {@link StructuredRecord} to {@link BSONWritable}.
 */
public class RecordToBSONWritableTransformer {

  /**
   * Transforms given {@link StructuredRecord} to {@link BSONWritable}.
   *
   * @param record structured record to be transformed.
   * @return {@link BSONWritable} that corresponds to the given {@link StructuredRecord}.
   */
  public BSONWritable transform(StructuredRecord record) {
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(), "Schema fields cannot be empty");
    BasicDBObjectBuilder bsonBuilder = BasicDBObjectBuilder.start();
    for (Schema.Field field : fields) {
      bsonBuilder.add(field.getName(), record.get(field.getName()));
    }
    return new BSONWritable(bsonBuilder.get());
  }
}

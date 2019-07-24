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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import java.util.List;
import java.util.Objects;

/**
 * Transforms {@link BSONObject} to {@link StructuredRecord}.
 */
public class BSONObjectToRecordTransformer {

  private final Schema schema;

  public BSONObjectToRecordTransformer(Schema schema) {
    this.schema = schema;
  }

  public StructuredRecord transform(BSONObject bsonObject) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema.getFields(), "Schema fields cannot be empty");
    for (Schema.Field field : fields) {
      builder.set(field.getName(), extractValue(bsonObject.get(field.getName()), field.getSchema()));
    }
    return builder.build();
  }

  private Object extractValue(Object object, Schema schema) {
    if (schema.isNullable()) {
      if (object == null) {
        return null;
      }
      schema = schema.getNonNullable();
    }
    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case ARRAY:
        return convertArray(object, schema.getComponentSchema());
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
        return object;
      default:
        throw new UnexpectedFormatException("field type " + fieldType + " is not supported.");
    }
  }

  private Object convertArray(Object object, Schema schema) {
    BasicBSONList bsonList = (BasicBSONList) object;
    List<Object> values = Lists.newArrayListWithCapacity(bsonList.size());
    for (Object obj : bsonList) {
      values.add(extractValue(obj, schema));
    }
    return values;
  }
}

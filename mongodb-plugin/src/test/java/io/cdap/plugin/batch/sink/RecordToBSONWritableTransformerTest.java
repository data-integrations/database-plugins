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

import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.BSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * {@link RecordToBSONWritableTransformer} test.
 */
public class RecordToBSONWritableTransformerTest {

  private static final RecordToBSONWritableTransformer TRANSFORMER = new RecordToBSONWritableTransformer();

  @Test
  public void testTransform() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("array_field",
                                                    Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("int_field", 15)
      .set("long_field", 10L)
      .set("double_field", 10.5D)
      .set("float_field", 15.5F)
      .set("string_field", "string_value")
      .set("boolean_field", true)
      .set("bytes_field", "test_blob".getBytes())
      .set("null_field", null)
      .set("array_field", Arrays.asList(1L, null, 2L, null, 3L))
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertEquals(inputRecord.get("int_field"), bsonObject.get("int_field"));
    Assert.assertEquals(inputRecord.get("long_field"), bsonObject.get("long_field"));
    Assert.assertEquals(inputRecord.get("double_field"), bsonObject.get("double_field"));
    Assert.assertEquals(inputRecord.get("float_field"), bsonObject.get("float_field"));
    Assert.assertEquals(inputRecord.get("string_field"), bsonObject.get("string_field"));
    Assert.assertEquals(inputRecord.get("boolean_field"), bsonObject.get("boolean_field"));
    Assert.assertEquals(inputRecord.get("bytes_field"), bsonObject.get("bytes_field"));
    Assert.assertNull(bsonObject.get("null_field"));
    Assert.assertEquals(inputRecord.get("array_field"), bsonObject.get("array_field"));
  }
}

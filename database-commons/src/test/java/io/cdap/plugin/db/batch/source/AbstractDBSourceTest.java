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

package io.cdap.plugin.db.batch.source;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for source schema validation.
 */
public class AbstractDBSourceTest {
  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("string_column", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("bytes_column", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
    Schema.Field.of("int_column", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("long_column", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("float_column", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
    Schema.Field.of("double_column", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("boolean_column", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))
  );

  @Test
  public void testValidateSourceSchemaCorrectSchema() {
    AbstractDBSource.DBSourceConfig.validateSchema(SCHEMA, SCHEMA);
  }

  @Test
  public void testValidateSourceSchemaMismatchFields() {
    Schema actualSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("string_column", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("bytes_column", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("int_column", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long_column", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float_column", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double_column", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
    );

    try {
      AbstractDBSource.DBSourceConfig.validateSchema(actualSchema, SCHEMA);
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(AbstractDBSource.DBSourceConfig.SCHEMA, e.getProperty());
    }
  }

  @Test
  public void testValidateSourceSchemaInvalidFieldType() {
    Schema actualSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("string_column", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("bytes_column", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("int_column", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long_column", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float_column", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double_column", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("boolean_column", Schema.nullableOf(Schema.of(Schema.Type.INT)))
    );

    try {
      AbstractDBSource.DBSourceConfig.validateSchema(actualSchema, SCHEMA);
      Assert.fail(String.format("Expected to throw %s", IllegalArgumentException.class.getName()));
    } catch (IllegalArgumentException e) {
      String errorMessage = "Schema field 'boolean_column' has type 'BOOLEAN' but found 'INT' in input record";
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }
}

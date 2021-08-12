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
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Test class for source schema validation.
 */
public class AbstractDBSourceTest {
  private static final String MOCK_STAGE = "mockStage";
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
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    AbstractDBSource.DBSourceConfig.validateSchema(SCHEMA, SCHEMA, collector,
                                                   AbstractDBSource.DBSourceConfig.FIELD_VALIDATOR);
    Assert.assertEquals(0, collector.getValidationFailures().size());
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

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    AbstractDBSource.DBSourceConfig.validateSchema(actualSchema, SCHEMA, collector,
                                                   AbstractDBSource.DBSourceConfig.FIELD_VALIDATOR);
    assertPropertyValidationFailed(collector, "boolean_column");
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

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    AbstractDBSource.DBSourceConfig.validateSchema(actualSchema, SCHEMA, collector,
                                                   AbstractDBSource.DBSourceConfig.FIELD_VALIDATOR);
    assertPropertyValidationFailed(collector, "boolean_column");
  }

  private static void assertPropertyValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.OUTPUT_SCHEMA_FIELD);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}

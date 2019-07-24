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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.MongoDBConstants;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests of {@link MongoDBBatchSource.MongoDBSourceConfig} methods.
 */
public class MongoDBSourceConfigTest {

  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("array_field", Schema.nullableOf(
                      Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))));

  private static final MongoDBBatchSource.MongoDBSourceConfig VALID_CONFIG = MongoDBSourceConfigBuilder.builder()
    .setReferenceName("MongoDBSource")
    .setHost("localhost")
    .setPort(27017)
    .setDatabase("admin")
    .setCollection("analytics")
    .setUser("admin")
    .setPassword("password")
    .setConnectionArguments("key=value;")
    .setSchema(VALID_SCHEMA.toString())
    .build();

  @Test
  public void testConfigConnectionString() {
    Assert.assertEquals("mongodb://admin:password@localhost:27017/admin.analytics?key=value;",
                        VALID_CONFIG.getConnectionString());
  }

  @Test
  public void testConfigConnectionStringNoCreds() {
    String connectionString = MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
      .setUser(null)
      .setPassword(null)
      .build()
      .getConnectionString();

    Assert.assertEquals("mongodb://localhost:27017/admin.analytics?key=value;", connectionString);
  }

  @Test
  public void testValidateValid() {
    VALID_CONFIG.validate();
  }

  @Test
  public void testValidateSchemaNull() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setSchema(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.SCHEMA, e.getProperty());
    }
  }

  @Test
  public void testValidateSchemaEmpty() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setSchema("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.SCHEMA, e.getProperty());
    }
  }

  @Test
  public void testValidateSchemaInvalidJson() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setSchema("not a json")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.SCHEMA, e.getProperty());
    }
  }

  @Test
  public void testValidateSchemaUnsupportedType() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("union_field", Schema.unionOf(
      Schema.of(Schema.Type.LONG),
      Schema.of(Schema.Type.STRING)))
    );
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setSchema(schema.toString())
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.SCHEMA, e.getProperty());
    }
  }

  @Test
  public void testValidateReferenceNameNull() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, e.getProperty());
    }
  }

  @Test
  public void testValidateReferenceNameEmpty() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, e.getProperty());
    }
  }

  @Test
  public void testValidateReferenceNameInvalid() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("**********")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, e.getProperty());
    }
  }

  @Test
  public void testValidateHostNull() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setHost(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.HOST, e.getProperty());
    }
  }

  @Test
  public void testValidateHostEmpty() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setHost("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.HOST, e.getProperty());
    }
  }

  @Test
  public void testValidatePortNull() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setPort(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.PORT, e.getProperty());
    }
  }

  @Test
  public void testValidatePortInvalid() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setPort(0)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.PORT, e.getProperty());
    }
  }

  @Test
  public void testValidateDatabaseNull() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setDatabase(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.DATABASE, e.getProperty());
    }
  }

  @Test
  public void testValidateDatabaseEmpty() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setDatabase("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.DATABASE, e.getProperty());
    }
  }

  @Test
  public void testValidateCollectionNull() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setCollection(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.COLLECTION, e.getProperty());
    }
  }

  @Test
  public void testValidateCollectionEmpty() {
    try {
      MongoDBSourceConfigBuilder.builder(VALID_CONFIG)
        .setCollection("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(MongoDBConstants.COLLECTION, e.getProperty());
    }
  }

  @Test
  public void testValidateEmpty() {
    try {
      new MongoDBBatchSource.MongoDBSourceConfig().validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      // Invalid config should have thrown exception
    }
  }
}

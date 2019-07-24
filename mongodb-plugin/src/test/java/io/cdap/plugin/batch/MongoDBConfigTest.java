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

package io.cdap.plugin.batch;

import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.MongoDBConfig;
import io.cdap.plugin.MongoDBConstants;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests of {@link MongoDBConfig} methods.
 */
public class MongoDBConfigTest {

  private static final MongoDBConfig VALID_CONFIG = MongoDBConfigBuilder.builder()
    .setReferenceName("MongoDBSource")
    .setHost("localhost")
    .setPort(27017)
    .setDatabase("admin")
    .setCollection("analytics")
    .setUser("admin")
    .setPassword("password")
    .setConnectionArguments("key=value;")
    .build();

  @Test
  public void testConfigConnectionString() {
    Assert.assertEquals("mongodb://admin:password@localhost:27017/admin.analytics?key=value;",
                        VALID_CONFIG.getConnectionString());
  }

  @Test
  public void testConfigConnectionStringNoCreds() {
    String connectionString = MongoDBConfigBuilder.builder(VALID_CONFIG)
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
  public void testValidateReferenceNameNull() {
    try {
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      MongoDBConfigBuilder.builder(VALID_CONFIG)
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
      new MongoDBConfig().validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      // Invalid config should have thrown exception
    }
  }
}

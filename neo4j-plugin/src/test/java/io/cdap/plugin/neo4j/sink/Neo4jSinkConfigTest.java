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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.neo4j.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * This is a test suite that cover Neo4j Source config validation.
 */
public class Neo4jSinkConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("age", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("dob", Schema.of(Schema.LogicalType.DATE)),
                    Schema.Field.of("profession", Schema.of(Schema.LogicalType.DATE)),
                    Schema.Field.of("company", Schema.of(Schema.LogicalType.DATE)),
                    Schema.Field.of("rating", Schema.of(Schema.LogicalType.DATE)),
                    Schema.Field.of("position", Schema.of(Schema.LogicalType.DATE))
    );
  private static final Neo4jSinkConfig VALID_CONFIG = new Neo4jSinkConfig(
    "ref_name",
    "localhost",
    7687,
    "user",
    "password",
    "CREATE (n:Test $(*))"
  );

  @Test
  public void testCheckValidConfig() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(collector, SCHEMA);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateQueryWithoutCreateKeywords() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSinkConfig config = Neo4jSinkConfig.builder(VALID_CONFIG)
      .setOutputQuery("MATCH (n:Test $(*))")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSinkConfig.NAME_OUTPUT_QUERY)
    );

    config.validate(collector, SCHEMA);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateQueryWithoutPropertyBlock() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSinkConfig config = Neo4jSinkConfig.builder(VALID_CONFIG)
      .setOutputQuery("CREATE (n:Test)")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSinkConfig.NAME_OUTPUT_QUERY)
    );

    config.validate(collector, SCHEMA);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateQueryWithEmptyPropertyBlock() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSinkConfig config = Neo4jSinkConfig.builder(VALID_CONFIG)
      .setOutputQuery("CREATE (n:Test $())")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSinkConfig.NAME_OUTPUT_QUERY)
    );

    config.validate(collector, SCHEMA);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateQueryWithWithInvalidSingleProperty() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSinkConfig config = Neo4jSinkConfig.builder(VALID_CONFIG)
      .setOutputQuery("CREATE (n:Test $(test))")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSinkConfig.NAME_OUTPUT_QUERY)
    );

    config.validate(collector, SCHEMA);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateQueryWithWithInvalidProperty() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSinkConfig config = Neo4jSinkConfig.builder(VALID_CONFIG)
      .setOutputQuery("CREATE (n:Test $(id, test))")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSinkConfig.NAME_OUTPUT_QUERY)
    );

    config.validate(collector, SCHEMA);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateComplexQuery() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSinkConfig config = Neo4jSinkConfig.builder(VALID_CONFIG)
      .setOutputQuery(
        "CREATE (n:Person $(name, dob, profession)-[r:Work_In $(position)->(c:Company $(company, rating)))")
      .build();

    config.validate(collector, SCHEMA);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

}

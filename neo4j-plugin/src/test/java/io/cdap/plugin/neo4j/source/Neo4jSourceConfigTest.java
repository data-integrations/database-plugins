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

package io.cdap.plugin.neo4j.source;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.neo4j.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This is a test suite that cover Neo4j Source config validation.
 */
public class Neo4jSourceConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Neo4jSourceConfig VALID_CONFIG = new Neo4jSourceConfig(
    "ref_name",
    "localhost",
    7687,
    "user",
    "password",
    "MATCH (n:Test) RETURN n",
    1,
    null
  );

  @Test
  public void testCheckValidConfig() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(collector);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateQueryWithUnavailableKeywords() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setInputQuery("MERGE (robert:Critic) RETURN robert, labels(robert)")
      .build();
    List<List<String>> paramNames = Arrays.asList(
      Collections.singletonList(Neo4jSourceConfig.NAME_INPUT_QUERY),
      Collections.singletonList(Neo4jSourceConfig.NAME_INPUT_QUERY)
    );

    config.validate(collector);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateQueryWithoutRequiredKeywords() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setInputQuery("MATCH (robert:Critic)")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSourceConfig.NAME_INPUT_QUERY)
    );

    config.validate(collector);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateInvalidSplitNumber() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setSplitNum(-2)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSourceConfig.NAME_SPLIT_NUM)
    );

    config.validate(collector);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateSplitNumberWithoutOrderBy() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setSplitNum(10)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(Neo4jSourceConfig.NAME_ORDER_BY)
    );

    config.validate(collector);
    ValidationAssertions.assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateSplitNumberWithOrderBy() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setSplitNum(10)
      .setOrderBy("n.id")
      .build();

    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateSplitNumberWithOrderByInQuery() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setSplitNum(10)
      .setInputQuery("MATCH (n:Test) RETURN n ORDER BY n.id")
      .build();

    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

}

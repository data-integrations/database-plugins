/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.postgres;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link PostgresConnector}
 */
public class PostgresConnectorUnitTest {
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  /**
   * Unit test for getTableName()
   */
  @Test
  public void getTableNameTest() {
    PostgresConnector connector = new PostgresConnector(null);
    Assert.assertEquals("\"schema\".\"table\"",
                        connector.getTableName("db", "schema", "table"));
  }

  /**
   * Unit tests for getTableQuery()
   */
  @Test
  public void getTableQueryTest() {
    PostgresConnector connector = new PostgresConnector(null);
    String tableName = connector.getTableName("db", "schema", "table");

    // default query, sampleType "first"
    Assert.assertEquals(String.format("SELECT * FROM %s LIMIT 100", tableName),
                        connector.getTableQuery("db", "schema", "table",
                                                100, "first", null));
    // default query, sampleType null
    Assert.assertEquals(String.format("SELECT * FROM %s LIMIT 100", tableName),
                        connector.getTableQuery("db", "schema", "table",
                                                100, null, null));
    // random query
    Assert.assertEquals(String.format("SELECT * FROM %s\n" +
                                        "TABLESAMPLE BERNOULLI (100.0 * %d / (SELECT COUNT(*) FROM %s))",
                                      tableName, 100, tableName),
                        connector.getTableQuery("db", "schema", "table",
                                                100, "random", null));
    // stratified query
    Assert.assertEquals(String.format("SELECT * FROM %s\n" +
                                        "TABLESAMPLE BERNOULLI (100.0 * %d / (SELECT COUNT(*) FROM %s))\n" +
                                        "ORDER BY %s",
                                      tableName, 100, tableName, "strata"),
                        connector.getTableQuery("db", "schema", "table",
                                                100, "stratified", "strata"));
  }

  /**
   * Test for null strata exception
   *
   * @throws IllegalArgumentException
   */
  @Test
  public void getTableQueryNullStrataTest() throws IllegalArgumentException {
    expectedEx.expect(IllegalArgumentException.class);
    PostgresConnector connector = new PostgresConnector(null);
    connector.getTableQuery("db", "schema", "table", 100, "stratified", null);
  }
}

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

package io.cdap.plugin.mssql;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link SqlServerConnector}
 */
public class SqlServerConnectorUnitTest {
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private static final SqlServerConnector CONNECTOR = new SqlServerConnector(null);

  /**
   * Unit tests for getTableQuery()
   */
  @Test
  public void getTableQueryTest() {
    String tableName = "\"db\".\"schema\".\"table\"";

    // default query, sampleType "first"
    Assert.assertEquals(String.format("SELECT TOP %d * FROM %s", 100, tableName),
                        CONNECTOR.getTableQuery("db", "schema", "table",
                                                100, "first", null));
    // default query, sampleType null
    Assert.assertEquals(String.format("SELECT TOP %d * FROM %s", 100, tableName),
                        CONNECTOR.getTableQuery("db", "schema", "table",
                                                100, null, null));
    // random query
    Assert.assertEquals(String.format("SELECT * FROM %s " +
                                        "WHERE (ABS(CAST((BINARY_CHECKSUM(*) * RAND()) as int)) %% 100) " +
                                        "< %d / (SELECT COUNT(*) FROM %s)",
                                      tableName, 10000, tableName),
                        CONNECTOR.getTableQuery("db", "schema", "table",
                                                100, "random", null));
    // sorted random query
    Assert.assertEquals(String.format("SELECT * FROM %s " +
                                        "WHERE (ABS(CAST((BINARY_CHECKSUM(*) * RAND()) as int)) %% 100) " +
                                        "< %d / (SELECT COUNT(*) FROM %s)" +
                                        "ORDER BY %s",
                                      tableName, 10000, tableName, "strata"),
                        CONNECTOR.getTableQuery("db", "schema", "table",
                                                100, "random", "strata"));
  }
}

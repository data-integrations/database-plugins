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

package io.cdap.plugin.oracle;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OracleConnectorUnitTest {
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private static final OracleConnector CONNECTOR = new OracleConnector(null);

  /**
   * Unit test for getTableName()
   */
  @Test
  public void getTableNameTest() {
    Assert.assertEquals("\"schema\".\"table\"",
                        CONNECTOR.getTableName("db", "schema", "table"));
  }

  /**
   * Unit tests for getTableQuery()
   */
  @Test
  public void getTableQueryTest() {
    String tableName = CONNECTOR.getTableName("db", "schema", "table");

    // default query
    Assert.assertEquals(String.format("SELECT * FROM %s WHERE ROWNUM <= %d", tableName, 100),
                        CONNECTOR.getTableQuery("db", "schema", "table",
                                                100));

    // random query
    Assert.assertEquals(String.format("SELECT * FROM (\n" +
                                        "SELECT * FROM %s ORDER BY DBMS_RANDOM.RANDOM\n" +
                                        ")\n" +
                                        "WHERE rownum <= %d",
                                      tableName, 100),
                        CONNECTOR.getRandomQuery(tableName, 100));
  }
}

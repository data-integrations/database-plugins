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

package io.cdap.plugin.mysql;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link MysqlConnector}
 */
public class MysqlConnectorUnitTest {
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private static final MysqlConnectorConfig CONFIG =
    new MysqlConnectorConfig("host", 3306, "user", "password", "mysql", null);

  private static final MysqlConnector CONNECTOR = new MysqlConnector(CONFIG);

  /**
   * Unit test for getTableName()
   */
  @Test
  public void getTableNameTest() {
    Assert.assertEquals("`db`.`table`",
                        CONNECTOR.getTableName("db", "schema", "table"));
  }

  /**
   * Unit test for getTableName() with identifiers containing backticks
   */
  @Test
  public void getTableNameEscapesBackticksTest() {
    Assert.assertEquals("`my``db`.`t``; DELETE FROM x; --`",
                        CONNECTOR.getTableName("my`db", "schema", "t`; DELETE FROM x; --"));
  }

  /**
   * Unit test for getStratifiedQuery() with a strata column containing backticks
   */
  @Test
  public void getStratifiedQueryEscapesStrataTest() {
    String query = CONNECTOR.getStratifiedQuery("`db`.`table`", 10, "id`; DROP TABLE t; --", "s1");
    Assert.assertTrue(query.contains("ORDER BY `id``; DROP TABLE t; --`, RAND()"));
    Assert.assertTrue(query.contains("ORDER BY `id``; DROP TABLE t; --`\n"));
  }

  /**
   * Unit tests for getTableQuery()
   */
  @Test
  public void getTableQueryTest() {
    String tableName = CONNECTOR.getTableName("db", "schema", "table");

    // random query
    Assert.assertEquals(String.format("SELECT * FROM %s\n" +
                                        "WHERE rand() < %d.0 / (SELECT COUNT(*) FROM %s)",
                                      tableName, 100, tableName),
                        CONNECTOR.getRandomQuery(tableName, 100));
  }
}

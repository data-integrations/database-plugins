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
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Unit tests for {@link PostgresConnector}
 */
public class PostgresConnectorUnitTest {
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private static final PostgresConnector CONNECTOR = new PostgresConnector(new PostgresConnectorConfig(
    "localhost", 5432, "user", "password", "postgresql",
    ""));

  /**
   * Unit test for getTableName()
   */
  @Test
  public void getTableNameTest() {
    Assert.assertEquals("\"schema\".\"table\"",
                        CONNECTOR.getTableName("db", "schema", "table"));
    Assert.assertEquals("\"sch\"\"ema\".\"ta\"\"ble\"",
                        CONNECTOR.getTableName("db", "sch\"ema", "ta\"ble"));
    Assert.assertEquals("\"pg_catalog\".\"pg_class\"\" LIMIT 0; UPDATE pg_catalog.pg_class SET relname = 'x'; --\"",
                        CONNECTOR.getTableName(
                          "db", "pg_catalog", "pg_class\" LIMIT 0; UPDATE pg_catalog.pg_class SET relname = 'x'; --"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTableNameRejectsNulTest() {
    CONNECTOR.getTableName("db", "schema", "ta\0ble");
  }

  /**
   * Unit tests for getTableQuery()
   */
  @Test
  public void getTableQueryTest() {
    String tableName = CONNECTOR.getTableName("db", "schema", "table");

    // random query
    Assert.assertEquals(String.format("SELECT * FROM %s\n" +
                                        "TABLESAMPLE BERNOULLI (100.0 * %d / (SELECT COUNT(*) FROM %s))",
                                      tableName, 100, tableName),
                        CONNECTOR.getRandomQuery(tableName, 100));

    // stratified query quotes and escapes strata column
    String stratifiedQuery = CONNECTOR.getStratifiedQuery(tableName, 100, "col\"1; DROP TABLE x; --", "s");
    Assert.assertTrue(stratifiedQuery.contains("ORDER BY \"col\"\"1; DROP TABLE x; --\", random()"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getStratifiedQueryRejectsNulInStrataTest() {
    CONNECTOR.getStratifiedQuery("\"schema\".\"table\"", 100, "col\0name", "s");
  }

  @Test
  public void validateTablePassesNullCatalogTest() throws SQLException {
    Connection connection = Mockito.mock(Connection.class);
    DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
    ResultSet tables = Mockito.mock(ResultSet.class);
    Mockito.when(connection.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getTables(null, "schema", "table", null)).thenReturn(tables);
    Mockito.when(tables.next()).thenReturn(true);

    CONNECTOR.validateTable(connection, "db", "schema", "table");

    Mockito.verify(metaData).getTables(null, "schema", "table", null);
  }
}

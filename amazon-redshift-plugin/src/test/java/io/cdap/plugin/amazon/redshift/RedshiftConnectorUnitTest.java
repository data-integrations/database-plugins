/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.amazon.redshift;

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
 * Unit tests for {@link RedshiftConnector}
 */
public class RedshiftConnectorUnitTest {
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private static final RedshiftConnector CONNECTOR = new RedshiftConnector(new RedshiftConnectorConfig(
    "username", "password", "jdbc", "", "localhost",
    "db", 5432));

  /**
   * Unit test for getTableName()
   */
  @Test
  public void getTableNameTest() {
    Assert.assertEquals("\"schema\".\"table\"",
                        CONNECTOR.getTableName("db", "schema", "table"));
    Assert.assertEquals("\"sch\"\"ema\".\"ta\"\"ble\"",
                        CONNECTOR.getTableName("db", "sch\"ema", "ta\"ble"));
  }

  @Test
  public void getRandomQuery() {
    Assert.assertEquals("SELECT * FROM TestData\n" +
        "TABLESAMPLE BERNOULLI (100.0 * 10 / (SELECT COUNT(*) FROM TestData))",
      CONNECTOR.getRandomQuery("TestData", 10));
  }

  @Test
  public void getDBRecordType() {
    Assert.assertEquals("class io.cdap.plugin.amazon.redshift.RedshiftDBRecord",
      CONNECTOR.getDBRecordType().toString());
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

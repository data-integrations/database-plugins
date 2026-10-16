/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.cloudsql.postgres;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CloudSQLPostgreSQLConnector}.
 */
public class CloudSQLPostgreSQLConnectorUnitTest {

  private static final CloudSQLPostgreSQLConnector CONNECTOR = new CloudSQLPostgreSQLConnector(
    new CloudSQLPostgreSQLConnectorConfig("user", "password", "postgresql", "", "public",
                                          "project:region:instance", "db", null));

  @Test
  public void getTableNameQuotesAndEscapesIdentifiersTest() {
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
}

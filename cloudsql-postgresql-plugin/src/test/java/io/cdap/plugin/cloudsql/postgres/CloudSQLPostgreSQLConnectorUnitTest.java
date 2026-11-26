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
 * Unit tests for {@link CloudSQLPostgreSQLConnector}
 */
public class CloudSQLPostgreSQLConnectorUnitTest {

  private static final CloudSQLPostgreSQLConnector CONNECTOR = new CloudSQLPostgreSQLConnector(
    new CloudSQLPostgreSQLConnectorConfig("user", "password", "cloudsql-postgresql", "", "public",
                                          "project:region:instance", "db", null));

  /**
   * Unit test for getTableName()
   */
  @Test
  public void getTableNameTest() {
    Assert.assertEquals("\"schema\".\"table\"",
                        CONNECTOR.getTableName("db", "schema", "table"));
  }

  /**
   * Unit test for getTableName() with identifiers containing double quotes, using the payload from the
   * reported sample path SQL injection
   */
  @Test
  public void getTableNameEscapesQuotesTest() {
    String table = "pg_class\" LIMIT 0; UPDATE audit_vault.write_marker SET value='INJECTED_BY_SAMPLE_PATH' " +
      "WHERE id=1; SELECT * FROM \"pg_catalog\".\"pg_class";
    Assert.assertEquals("\"pg_catalog\".\"pg_class\"\" LIMIT 0; UPDATE audit_vault.write_marker SET " +
                          "value='INJECTED_BY_SAMPLE_PATH' WHERE id=1; SELECT * FROM \"\"pg_catalog\"\".\"\"pg_class\"",
                        CONNECTOR.getTableName("db", "pg_catalog", table));
    Assert.assertEquals("\"my\"\"schema\".\"table\"",
                        CONNECTOR.getTableName("db", "my\"schema", "table"));
  }
}

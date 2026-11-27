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

package io.cdap.plugin.cloudsql.mysql;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CloudSQLMySQLConnector}
 */
public class CloudSQLMySQLConnectorUnitTest {

  private static final CloudSQLMySQLConnector CONNECTOR = new CloudSQLMySQLConnector(
    new CloudSQLMySQLConnectorConfig("user", "password", "cloudsql-mysql", "", "public",
                                     "project:region:instance", "db", null));

  /**
   * Unit test for getTableName()
   */
  @Test
  public void getTableNameTest() {
    Assert.assertEquals("`db`.`table`",
                        CONNECTOR.getTableName("db", null, "table"));
  }

  /**
   * Unit test for getTableName() with identifiers containing backticks
   */
  @Test
  public void getTableNameEscapesBackticksTest() {
    Assert.assertEquals("`my``db`.`t``; DELETE FROM x; --`",
                        CONNECTOR.getTableName("my`db", null, "t`; DELETE FROM x; --"));
  }
}

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

package io.cdap.plugin.databricks;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DatabricksConnector}
 */
public class DatabricksConnectorUnitTest {

  private static final DatabricksConnector CONNECTOR = new DatabricksConnector(new DatabricksConnectorConfig(
    "token", "password", "jdbc", "", "dbc-xxx.cloud.databricks.com",
    "sql/1.0/warehouses/xxx", "main", 443));

  @Test
  public void testGetTableName() {
    Assert.assertEquals("`main`.`default`.`my_table`",
                        CONNECTOR.getTableName("main", "default", "my_table"));
    Assert.assertEquals("`default`.`my_table`",
                        CONNECTOR.getTableName(null, "default", "my_table"));
    Assert.assertEquals("`my_table`",
                        CONNECTOR.getTableName(null, null, "my_table"));
  }

  @Test
  public void testGetRandomQuery() {
    Assert.assertEquals("SELECT * FROM `main`.`default`.`my_table` LIMIT 10",
                        CONNECTOR.getRandomQuery("`main`.`default`.`my_table`", 10));
  }

  @Test
  public void testGetDBRecordType() {
    Assert.assertEquals("class io.cdap.plugin.databricks.DatabricksDBRecord",
                        CONNECTOR.getDBRecordType().toString());
  }

  @Test
  public void testConnectionString() {
    DatabricksConnectorConfig config = new DatabricksConnectorConfig(
      "token", "secret", "jdbc", "", "dbc-xxx.cloud.databricks.com",
      "sql/1.0/warehouses/xxx", "main", 443);
    Assert.assertEquals(
      "jdbc:databricks://dbc-xxx.cloud.databricks.com:443/main;HttpPath=sql/1.0/warehouses/xxx;",
      config.getConnectionString());

    DatabricksConnectorConfig configNoDb = new DatabricksConnectorConfig(
      "token", "secret", "jdbc", "", "dbc-xxx.cloud.databricks.com",
      "sql/1.0/warehouses/xxx", null, 443);
    Assert.assertEquals(
      "jdbc:databricks://dbc-xxx.cloud.databricks.com:443;HttpPath=sql/1.0/warehouses/xxx;",
      configNoDb.getConnectionString());
  }

}

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

package io.cdap.plugin.jdbc;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DatabaseSource}
 */
public class DatabaseSourceTest {

  @Test
  public void testUseSquareBracketQuotes() {
    Assert.assertTrue(createSource("jdbc:sqlserver://localhost:1433;databaseName=db").useSquareBracketQuotes());
    Assert.assertTrue(createSource("jdbc:jtds:sqlserver://localhost:1433/db").useSquareBracketQuotes());
    Assert.assertTrue(createSource("JDBC:SQLSERVER://localhost:1433").useSquareBracketQuotes());
    Assert.assertFalse(createSource("jdbc:postgresql://localhost:5432/db").useSquareBracketQuotes());
    Assert.assertFalse(createSource("jdbc:mysql://localhost:3306/db").useSquareBracketQuotes());
    Assert.assertFalse(createSource(null).useSquareBracketQuotes());
  }

  private static DatabaseSource createSource(String connectionString) {
    DatabaseSource.DatabaseSourceConfig config = new DatabaseSource.DatabaseSourceConfig();
    config.connectionString = connectionString;
    return new DatabaseSource(config);
  }
}

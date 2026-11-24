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

import io.cdap.plugin.util.SelectQueryValidator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

/**
 * Unit tests for {@link DatabaseSource}.
 */
public class DatabaseSourceTest {

  @Test
  public void testGetSqlDialect() {
    Assert.assertEquals(SelectQueryValidator.Dialect.TSQL,
                        createSource("jdbc:sqlserver://localhost:1433;databaseName=db", null).getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.TSQL,
                        createSource("jdbc:jtds:sqlserver://localhost:1433/db", null).getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.TSQL,
                        createSource("JDBC:SQLSERVER://localhost:1433", null).getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.POSTGRESQL,
                        createSource("jdbc:postgresql://localhost:5432/db", null).getSqlDialect());
    Assert.assertEquals(
      SelectQueryValidator.Dialect.UNKNOWN,
      createSource("jdbc:postgresql://localhost:5432/db", "options=-c standard_conforming_strings=off")
        .getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.MYSQL,
                        createSource("jdbc:mysql://localhost:3306/db", null).getSqlDialect());
    Assert.assertEquals(
      SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH,
      createSource("jdbc:mysql://localhost:3306/db", "sessionVariables=sql_mode='NO_BACKSLASH_ESCAPES'")
        .getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.DATABRICKS,
                        createSource("jdbc:databricks://host:443/default", null).getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.ANSI,
                        createSource("jdbc:oracle:thin:@localhost:1521:xe", null).getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.ANSI,
                        createSource("jdbc:hsqldb:hsql://localhost/testdb", null).getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.UNKNOWN,
                        createSource("jdbc:redshift://cluster:5439/dev", null).getSqlDialect());
    Assert.assertEquals(SelectQueryValidator.Dialect.UNKNOWN,
                        createSource(null, null).getSqlDialect());
  }

  @Test
  public void testGetSqlDialectTurkishLocale() {
    Locale previous = Locale.getDefault();
    Locale.setDefault(new Locale("tr", "TR"));
    try {
      Assert.assertEquals(SelectQueryValidator.Dialect.TSQL,
                          createSource("JDBC:SQLSERVER://localhost:1433", null).getSqlDialect());
      Assert.assertEquals(
        SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH,
        createSource("JDBC:MYSQL://localhost:3306/db", "SESSIONVARIABLES=sql_mode='NO_BACKSLASH_ESCAPES'")
          .getSqlDialect());
    } finally {
      Locale.setDefault(previous);
    }
  }

  private static DatabaseSource createSource(String connectionString, String connectionArguments) {
    DatabaseSource.DatabaseSourceConfig config = new DatabaseSource.DatabaseSourceConfig();
    config.connectionString = connectionString;
    config.connectionArguments = connectionArguments;
    return new DatabaseSource(config);
  }
}

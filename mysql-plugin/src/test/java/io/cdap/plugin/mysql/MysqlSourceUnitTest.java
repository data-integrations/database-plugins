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

package io.cdap.plugin.mysql;

import io.cdap.plugin.util.SelectQueryValidator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MysqlSource} SQL dialect selection.
 */
public class MysqlSourceUnitTest {

  @Test
  public void testGetSqlDialectSwitchesToAmbiguousBackslashWhenSqlModeMayBeOverridden() {
    MysqlSource.MysqlSourceConfig defaultConfig = new MysqlSource.MysqlSourceConfig();
    defaultConfig.referenceName = "mysqlRef";
    Assert.assertEquals(SelectQueryValidator.Dialect.MYSQL, new MysqlSource(defaultConfig).getSqlDialect());

    MysqlSource.MysqlSourceConfig withSqlMode = new MysqlSource.MysqlSourceConfig();
    withSqlMode.referenceName = "mysqlRef";
    withSqlMode.sqlMode = "NO_BACKSLASH_ESCAPES";
    Assert.assertEquals(SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH,
                        new MysqlSource(withSqlMode).getSqlDialect());

    MysqlConnectorConfig connWithSessionVars = new MysqlConnectorConfig(
      "localhost", 3306, "user", "pass", "mysql", "sessionVariables=sql_mode='NO_BACKSLASH_ESCAPES'");
    MysqlSource.MysqlSourceConfig withConnArgs = new MysqlSource.MysqlSourceConfig() {
      @Override
      public MysqlConnectorConfig getConnection() {
        return connWithSessionVars;
      }
    };
    withConnArgs.referenceName = "mysqlRef";
    Assert.assertEquals(SelectQueryValidator.Dialect.MYSQL_AMBIGUOUS_BACKSLASH,
                        new MysqlSource(withConnArgs).getSqlDialect());
  }
}

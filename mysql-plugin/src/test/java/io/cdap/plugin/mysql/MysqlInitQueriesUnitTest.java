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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Unit tests for {@code getInitQueries()} escaping across MySQL plugin configs.
 */
public class MysqlInitQueriesUnitTest {

  @Test
  public void getInitQueries_emptyOrNullSqlMode_returnsEmptyList() {
    MysqlSource.MysqlSourceConfig sourceConfig = new MysqlSource.MysqlSourceConfig();
    MysqlSink.MysqlSinkConfig sinkConfig = new MysqlSink.MysqlSinkConfig();
    MysqlAction.MysqlActionConfig actionConfig = new MysqlAction.MysqlActionConfig();
    MysqlPostAction.MysqlQueryActionConfig postActionConfig =
      new MysqlPostAction.MysqlQueryActionConfig();

    Assert.assertEquals(Collections.emptyList(), sourceConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), sinkConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), actionConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), postActionConfig.getInitQueries());

    sourceConfig.sqlMode = "";
    sinkConfig.sqlMode = "";
    actionConfig.sqlMode = "";
    postActionConfig.sqlMode = "";

    Assert.assertEquals(Collections.emptyList(), sourceConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), sinkConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), actionConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), postActionConfig.getInitQueries());
  }

  @Test
  public void getInitQueries_plainSqlModeAndAnsiQuotes_formatsInitQueries() {
    String expectedSqlMode = "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE';";

    MysqlSource.MysqlSourceConfig sourceConfig = new MysqlSource.MysqlSourceConfig();
    sourceConfig.useAnsiQuotes = true;
    sourceConfig.sqlMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE";
    Assert.assertEquals(Arrays.asList(MysqlConstants.ANSI_QUOTES_QUERY, expectedSqlMode),
                        sourceConfig.getInitQueries());

    MysqlSink.MysqlSinkConfig sinkConfig = new MysqlSink.MysqlSinkConfig();
    sinkConfig.sqlMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE";
    Assert.assertEquals(Collections.singletonList(expectedSqlMode), sinkConfig.getInitQueries());

    MysqlAction.MysqlActionConfig actionConfig = new MysqlAction.MysqlActionConfig();
    actionConfig.useAnsiQuotes = true;
    actionConfig.sqlMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE";
    Assert.assertEquals(Arrays.asList(MysqlConstants.ANSI_QUOTES_QUERY, expectedSqlMode),
                        actionConfig.getInitQueries());

    MysqlPostAction.MysqlQueryActionConfig postActionConfig =
      new MysqlPostAction.MysqlQueryActionConfig();
    postActionConfig.useAnsiQuotes = true;
    postActionConfig.sqlMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE";
    Assert.assertEquals(Arrays.asList(MysqlConstants.ANSI_QUOTES_QUERY, expectedSqlMode),
                        postActionConfig.getInitQueries());
  }

  @Test
  public void getInitQueries_quotesAndBackslashes_escapesBackslashesBeforeQuotes() {
    String payload = "TRADITIONAL\\'; DELETE FROM t; --";
    String expected = "SET SESSION sql_mode = 'TRADITIONAL\\\\''; DELETE FROM t; --';";

    MysqlSource.MysqlSourceConfig sourceConfig = new MysqlSource.MysqlSourceConfig();
    sourceConfig.sqlMode = payload;
    Assert.assertEquals(Collections.singletonList(expected), sourceConfig.getInitQueries());

    MysqlSink.MysqlSinkConfig sinkConfig = new MysqlSink.MysqlSinkConfig();
    sinkConfig.sqlMode = payload;
    Assert.assertEquals(Collections.singletonList(expected), sinkConfig.getInitQueries());

    MysqlAction.MysqlActionConfig actionConfig = new MysqlAction.MysqlActionConfig();
    actionConfig.sqlMode = payload;
    Assert.assertEquals(Collections.singletonList(expected), actionConfig.getInitQueries());

    MysqlPostAction.MysqlQueryActionConfig postActionConfig =
      new MysqlPostAction.MysqlQueryActionConfig();
    postActionConfig.sqlMode = payload;
    Assert.assertEquals(Collections.singletonList(expected), postActionConfig.getInitQueries());
  }
}

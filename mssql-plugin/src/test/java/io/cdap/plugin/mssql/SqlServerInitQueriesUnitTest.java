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

package io.cdap.plugin.mssql;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Unit tests for {@code getInitQueries()} escaping across SQL Server plugin configs.
 */
public class SqlServerInitQueriesUnitTest {

  @Test
  public void getInitQueries_emptyOrNullLanguage_returnsEmptyList() {
    SqlServerSource.SqlServerSourceConfig sourceConfig = new SqlServerSource.SqlServerSourceConfig();
    SqlServerSink.SqlServerSinkConfig sinkConfig = new SqlServerSink.SqlServerSinkConfig();
    SqlServerAction.SqlServerActionConfig actionConfig = new SqlServerAction.SqlServerActionConfig();
    SqlServerPostAction.SqlServerQueryActionConfig postActionConfig =
      new SqlServerPostAction.SqlServerQueryActionConfig();

    Assert.assertEquals(Collections.emptyList(), sourceConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), sinkConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), actionConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), postActionConfig.getInitQueries());

    sourceConfig.currentLanguage = "";
    sinkConfig.currentLanguage = "";
    actionConfig.currentLanguage = "";
    postActionConfig.currentLanguage = "";

    Assert.assertEquals(Collections.emptyList(), sourceConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), sinkConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), actionConfig.getInitQueries());
    Assert.assertEquals(Collections.emptyList(), postActionConfig.getInitQueries());
  }

  @Test
  public void getInitQueries_plainLanguage_formatsSetLanguageStatement() {
    String expected = "SET LANGUAGE 'us_english';";

    SqlServerSource.SqlServerSourceConfig sourceConfig = new SqlServerSource.SqlServerSourceConfig();
    sourceConfig.currentLanguage = "us_english";
    Assert.assertEquals(Collections.singletonList(expected), sourceConfig.getInitQueries());

    SqlServerSink.SqlServerSinkConfig sinkConfig = new SqlServerSink.SqlServerSinkConfig();
    sinkConfig.currentLanguage = "us_english";
    Assert.assertEquals(Collections.singletonList(expected), sinkConfig.getInitQueries());

    SqlServerAction.SqlServerActionConfig actionConfig = new SqlServerAction.SqlServerActionConfig();
    actionConfig.currentLanguage = "us_english";
    Assert.assertEquals(Collections.singletonList(expected), actionConfig.getInitQueries());

    SqlServerPostAction.SqlServerQueryActionConfig postActionConfig =
      new SqlServerPostAction.SqlServerQueryActionConfig();
    postActionConfig.currentLanguage = "us_english";
    Assert.assertEquals(Collections.singletonList(expected), postActionConfig.getInitQueries());
  }

  @Test
  public void getInitQueries_quotesAndBackslashes_doublesSingleQuotesOnly() {
    String payload = "us_english\\'; UPDATE t SET v = 'x'; --";
    String expected = "SET LANGUAGE 'us_english\\''; UPDATE t SET v = ''x''; --';";

    SqlServerSource.SqlServerSourceConfig sourceConfig = new SqlServerSource.SqlServerSourceConfig();
    sourceConfig.currentLanguage = payload;
    Assert.assertEquals(Collections.singletonList(expected), sourceConfig.getInitQueries());

    SqlServerSink.SqlServerSinkConfig sinkConfig = new SqlServerSink.SqlServerSinkConfig();
    sinkConfig.currentLanguage = payload;
    Assert.assertEquals(Collections.singletonList(expected), sinkConfig.getInitQueries());

    SqlServerAction.SqlServerActionConfig actionConfig = new SqlServerAction.SqlServerActionConfig();
    actionConfig.currentLanguage = payload;
    Assert.assertEquals(Collections.singletonList(expected), actionConfig.getInitQueries());

    SqlServerPostAction.SqlServerQueryActionConfig postActionConfig =
      new SqlServerPostAction.SqlServerQueryActionConfig();
    postActionConfig.currentLanguage = payload;
    Assert.assertEquals(Collections.singletonList(expected), postActionConfig.getInitQueries());
  }

  @Test
  public void buildSetLanguageQuery_trailingBackslashAndNullHandling() {
    Assert.assertEquals("SET LANGUAGE 'us_english\\';", SqlServerUtil.buildSetLanguageQuery("us_english\\"));
    Assert.assertThrows(NullPointerException.class, () -> SqlServerUtil.buildSetLanguageQuery(null));
  }
}

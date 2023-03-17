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

package io.cdap.plugin.common.stepsdesign;

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.PostgresqlClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;

import java.sql.SQLException;

/**
 * POSTGRESQL test hooks.
 */
public class TestSetupHooks {

  @Before(order = 1)
  public static void setTableName() {
    String randomString = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    String sourceTableName = String.format("sourcetable_%s", randomString);
    String targetTableName = String.format("targettable_%s", randomString);
    PluginPropertyUtils.addPluginProp("sourceTable", sourceTableName);
    PluginPropertyUtils.addPluginProp("targetTable", targetTableName);
    String schema = PluginPropertyUtils.pluginProp("schema");
    PluginPropertyUtils.addPluginProp("selectQuery",
                                      String.format("select * from %s.%s", schema, sourceTableName));
  }

  @Before(order = 2, value = "@POSTGRESQL_SOURCE_TEST")
  public static void createTables() throws SQLException, ClassNotFoundException {
    PostgresqlClient.createSourceTable(PluginPropertyUtils.pluginProp("sourceTable"),
                                       PluginPropertyUtils.pluginProp("schema"));
    PostgresqlClient.createTargetTable(PluginPropertyUtils.pluginProp("targetTable"),
                                       PluginPropertyUtils.pluginProp("schema"));
  }

  @After(order = 2, value = "@POSTGRESQL_SINK_TEST")
  public static void dropTables() throws SQLException, ClassNotFoundException {
    PostgresqlClient.dropTables(new String[]{PluginPropertyUtils.pluginProp("sourceTable"),
                                PluginPropertyUtils.pluginProp("targetTable")},
                                PluginPropertyUtils.pluginProp("schema"));
  }
}

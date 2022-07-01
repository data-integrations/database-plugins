/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.plugin.MysqlClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;

import java.sql.SQLException;

/**
 * MYSQL test hooks.
 */
public class TestSetupHooks {

  @Before(order = 1)
  public static void overrideUserAndPasswordIfProvided() {
    String username = System.getenv("username");
    if (username != null && !username.isEmpty()) {
        PluginPropertyUtils.addPluginProp("username", username);
    }
    String password = System.getenv("password");
    if (password != null && !password.isEmpty()) {
      PluginPropertyUtils.addPluginProp("password", password);
    }
  }

  @Before(order = 1, value = "@MYSQL_SOURCE_TEST")
  public static void setSelectQuery() {                
    String sourceTable =  PluginPropertyUtils.pluginProp("sourceTable");
    PluginPropertyUtils.addPluginProp("selectQuery",
                                      PluginPropertyUtils.pluginProp("selectQuery").
                                        replace("${table}", sourceTable));
  }

  @Before(order = 2, value = "@MYSQL_SOURCE_TEST")
  public static void createTables() throws SQLException, ClassNotFoundException {
    MysqlClient.createSourceTable(PluginPropertyUtils.pluginProp("sourceTable"));
    MysqlClient.createTargetTable(PluginPropertyUtils.pluginProp("targetTable"));
  }

  @After(order = 2, value = "@MYSQL_SINK_TEST")
  public static void dropTables() throws SQLException, ClassNotFoundException {
    MysqlClient.dropTables(new String[]{PluginPropertyUtils.pluginProp("sourceTable"),
      PluginPropertyUtils.pluginProp("targetTable")});
  }

}

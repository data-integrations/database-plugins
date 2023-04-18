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
import io.cdap.plugin.MssqlClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;

import java.sql.SQLException;

/**
 * Mssql test hooks.
 */
public class TestSetupHooks {

    @Before(order = 1)
    public static void setTableName() {
        String randomString = RandomStringUtils.randomAlphabetic(10).toUpperCase();
        String sourceTableName = String.format("SOURCETABLE_%s", randomString);
        String targetTableName = String.format("TARGETTABLE_%s", randomString);
        PluginPropertyUtils.addPluginProp("sourceTable", sourceTableName);
        PluginPropertyUtils.addPluginProp("targetTable", targetTableName);
        String schema = PluginPropertyUtils.pluginProp("schema");
        PluginPropertyUtils.addPluginProp("selectQuery", String.format("select * from %s.%s", schema,
                sourceTableName));
    }

    @Before(order = 2, value = "@MSSQL_SOURCE_TEST")
    public static void createTables() throws SQLException, ClassNotFoundException {
        MssqlClient.createSourceTable(PluginPropertyUtils.pluginProp("sourceTable"),
                PluginPropertyUtils.pluginProp("schema"));
        MssqlClient.createTargetTable(PluginPropertyUtils.pluginProp("targetTable"),
                PluginPropertyUtils.pluginProp("schema"));
    }

    @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_TEST")
    public static void createAllDatatypesTables() throws SQLException, ClassNotFoundException {
        MssqlClient.createSourceDatatypesTable(PluginPropertyUtils.pluginProp("sourceTable"),
                PluginPropertyUtils.pluginProp("schema"));
        MssqlClient.createTargetDatatypesTable(PluginPropertyUtils.pluginProp("targetTable"),
                PluginPropertyUtils.pluginProp("schema"));
    }

    @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_IMAGE_TEST")
    public static void createDatatypesTablesImage() throws SQLException, ClassNotFoundException {
        MssqlClient.createSourceImageTable(PluginPropertyUtils.pluginProp("sourceTable"),
                PluginPropertyUtils.pluginProp("schema"));
        MssqlClient.createTargetImageTable(PluginPropertyUtils.pluginProp("targetTable"),
                PluginPropertyUtils.pluginProp("schema"));
    }

    @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_UIDTYPE_TEST")
    public static void createDatatypesTablesUniqueIdentifier() throws SQLException, ClassNotFoundException {
        MssqlClient.createSourceUniqueIdentifierTable(PluginPropertyUtils.pluginProp("sourceTable"),
                PluginPropertyUtils.pluginProp("schema"));
        MssqlClient.createTargetUniqueIdentifierTable(PluginPropertyUtils.pluginProp("targetTable"),
                PluginPropertyUtils.pluginProp("schema"));
    }

    @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_DATETIME_TEST")
    public static void createDatatypesTablesDateTime() throws SQLException, ClassNotFoundException {
        MssqlClient.createSourceDateTimeTable(PluginPropertyUtils.pluginProp("sourceTable"),
                PluginPropertyUtils.pluginProp("schema"));
        MssqlClient.createTargetDateTimeTable(PluginPropertyUtils.pluginProp("targetTable"),
                PluginPropertyUtils.pluginProp("schema"));
    }

    @After(order = 1, value = "@MSSQL_SINK_TEST")
    public static void dropTables() throws SQLException, ClassNotFoundException {
        MssqlClient.deleteTables(PluginPropertyUtils.pluginProp("schema"),
                new String[]{PluginPropertyUtils.pluginProp("sourceTable"),
                        PluginPropertyUtils.pluginProp("targetTable")});
    }

}

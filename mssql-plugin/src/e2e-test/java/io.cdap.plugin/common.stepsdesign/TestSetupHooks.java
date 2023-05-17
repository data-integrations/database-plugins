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

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.MssqlClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Mssql test hooks.
 */
public class TestSetupHooks {
    public static String bqTargetTable = StringUtils.EMPTY;

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

    @Before(order = 2, value = "@MSSQL_TEST_TABLE")
    public static void createMssqlTestTable() throws SQLException, ClassNotFoundException {
        MssqlClient.createTargetMssqlTable(PluginPropertyUtils.pluginProp("targetTable"),
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

    @After(order = 1, value = "@MSSQL_SINK_TEST")
    public static void dropTables() throws SQLException, ClassNotFoundException {
        MssqlClient.deleteTables(PluginPropertyUtils.pluginProp("schema"),
                new String[]{PluginPropertyUtils.pluginProp("sourceTable"),
                        PluginPropertyUtils.pluginProp("targetTable")});
    }

    @Before(order = 1, value = "@BQ_SINK")
    public static void setTempTargetBQTable() {
        bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
        PluginPropertyUtils.addPluginProp("bqtarget.table", bqTargetTable);
        BeforeActions.scenario.write("BigQuery Target table name: " + bqTargetTable);
    }

    @After(order = 1, value = "@BQ_SINK_CLEANUP")
    public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
        try {
            BigQueryClient.dropBqQuery(bqTargetTable);
            BeforeActions.scenario.write("BigQuery Target table: " + bqTargetTable + " is deleted successfully");
            bqTargetTable = StringUtils.EMPTY;
        } catch (BigQueryException e) {
            if (e.getCode() == 404) {
                BeforeActions.scenario.write("BigQuery Target Table: " + bqTargetTable + " does not exist");
            } else {
                Assert.fail(e.getMessage());
            }
        }
    }

    /**
     * Create BigQuery table.
     */
    @Before(order = 1, value = "@BQ_SOURCE_TEST")
    public static void createTempSourceBQTable() throws IOException, InterruptedException {
        createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFile"),
                PluginPropertyUtils.pluginProp("InsertBQDataQueryFile"));
    }

    @After(order = 1, value = "@BQ_SOURCE_TEST")
    public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
        String bqSourceTable = PluginPropertyUtils.pluginProp("bqSourceTable");
        BigQueryClient.dropBqQuery(bqSourceTable);
        BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
        PluginPropertyUtils.removePluginProp("bqSourceTable");
    }

    private static void createSourceBQTableWithQueries(String bqCreateTableQueryFile, String bqInsertDataQueryFile)
            throws IOException, InterruptedException {
        String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().substring(0, 5).replaceAll("-",
                "_");
        System.out.println(bqSourceTable);

        String createTableQuery = StringUtils.EMPTY;
        try {
            createTableQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
                    ("/" + bqCreateTableQueryFile).toURI()))
                    , StandardCharsets.UTF_8);
            createTableQuery = createTableQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
                    .replace("TABLE_NAME", bqSourceTable);
        } catch (Exception e) {
            BeforeActions.scenario.write("Exception in reading " + bqCreateTableQueryFile + " - " + e.getMessage());
            Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading create table query file " + e.getMessage());
        }

        String insertDataQuery = StringUtils.EMPTY;
        try {
            insertDataQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
                    ("/" + bqInsertDataQueryFile).toURI()))
                    , StandardCharsets.UTF_8);
            insertDataQuery = insertDataQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
                    .replace("TABLE_NAME", bqSourceTable);
        } catch (Exception e) {
            BeforeActions.scenario.write("Exception in reading " + bqInsertDataQueryFile + " - " + e.getMessage());
            Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading insert data query file " + e.getMessage());
        }
        BigQueryClient.getSoleQueryResult(createTableQuery);
        try {
            BigQueryClient.getSoleQueryResult(insertDataQuery);
        } catch (NoSuchElementException e) {
            // Insert query does not return any record.
            // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
        }
        PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
        BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
    }
}

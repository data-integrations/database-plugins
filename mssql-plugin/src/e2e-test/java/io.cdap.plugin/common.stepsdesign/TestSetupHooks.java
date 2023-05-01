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


  @Before(order = 2, value = "@MSSQL_AS_SOURCE")
  public static void createSourceTables() throws SQLException, ClassNotFoundException {
    MssqlClient.createSourceTable(PluginPropertyUtils.pluginProp("sourceTable"),
                                  PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_AS_SOURCE")
  public static void dropSourceTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                            PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_AS_TARGET")
  public static void createTargetTables() throws SQLException, ClassNotFoundException {
    MssqlClient.createTargetTable(PluginPropertyUtils.pluginProp("targetTable"),
                                  PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_AS_TARGET")
  public static void dropTargetTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_TEST_TABLE")
  public static void createMssqlTestTable() throws SQLException, ClassNotFoundException {
    MssqlClient.createTargetMssqlTable(PluginPropertyUtils.pluginProp("targetTable"),
                                       PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " created successfully");
  }
  @After(order = 2, value = "@MSSQL_TEST_TABLE")
  public static void dropTestTargetTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_TEST")
  public static void createSourceDatatypesTables() throws SQLException, ClassNotFoundException {
    MssqlClient.createSourceDatatypesTable(PluginPropertyUtils.pluginProp("sourceTable"),
                                           PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_SOURCE_DATATYPES_TEST")
  public static void dropSourceDatatypesTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " deleted successfully");
  }


  @Before(order = 2, value = "@MSSQL_TARGET_DATATYPES_TEST")
  public static void createTargetDatatypesTables() throws SQLException, ClassNotFoundException {
    MssqlClient.createTargetDatatypesTable(PluginPropertyUtils.pluginProp("targetTable"),
                                           PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL Target DataTypes Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_TARGET_DATATYPES_TEST")
  public static void dropTargetDatatypesTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_IMAGE_TEST")
  public static void createSourceDatatypesTablesImage() throws SQLException, ClassNotFoundException {
    MssqlClient.createSourceImageTable(PluginPropertyUtils.pluginProp("sourceTable"),
                                       PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                     PluginPropertyUtils.pluginProp("sourceTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_SOURCE_DATATYPES_IMAGE_TEST")
  public static void dropSourceImageTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " deleted successfully");
  }
  @Before(order = 2, value = "@MSSQL_TARGET_DATATYPES_IMAGE_TEST")
  public static void createTargetDatatypesTablesImage() throws SQLException, ClassNotFoundException {
    MssqlClient.createTargetImageTable(PluginPropertyUtils.pluginProp("targetTable"),
                                       PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_TARGET_DATATYPES_IMAGE_TEST")
  public static void dropTargetImageTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_UIDTYPE_TEST")
  public static void createSourceDatatypesTablesUniqueIdentifier() throws SQLException, ClassNotFoundException {
    MssqlClient.createSourceUniqueIdentifierTable(PluginPropertyUtils.pluginProp("sourceTable"),
                                                  PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_SOURCE_DATATYPES_UIDTYPE_TEST")
  public static void dropSourceDatatypesTablesUniqueIdentifier() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MSSQL SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_TARGET_DATATYPES_UIDTYPE_TEST")
  public static void createTargetDatatypesTablesUniqueIdentifier() throws SQLException, ClassNotFoundException {
    MssqlClient.createTargetUniqueIdentifierTable(PluginPropertyUtils.pluginProp("targetTable"),
                                                  PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL UIDTYPE TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " created successfully");
  }
  @After(order = 2, value = "@MSSQL_TARGET_DATATYPES_UIDTYPE_TEST")
  public static void dropTargetDatatypesTablesUniqueIdentifier() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_SOURCE_DATATYPES_DATETIME_TEST")
  public static void createSourceDatatypesTablesDateTime() throws SQLException, ClassNotFoundException {
    MssqlClient.createSourceDateTimeTable(PluginPropertyUtils.pluginProp("sourceTable"),
                                          PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL UIDTYPE SOURCE Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " created successfully");

  }

  @After(order = 2, value = "@MSSQL_SOURCE_DATATYPES_DATETIME_TEST")
  public static void dropSourceDatatypesTablesDateTime() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("sourceTable") + " deleted successfully");
  }

  @Before(order = 2, value = "@MSSQL_TARGET_DATATYPES_DATETIME_TEST")
  public static void createTargetDatatypesTablesDateTime() throws SQLException, ClassNotFoundException {
    MssqlClient.createTargetDateTimeTable(PluginPropertyUtils.pluginProp("targetTable"),
                                          PluginPropertyUtils.pluginProp("schema"));
    BeforeActions.scenario.write("MSSQL UIDTYPE TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_SOURCE_DATATYPES_DATETIME_TEST")
  public static void dropTargetDatatypesTablesDateTime() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(PluginPropertyUtils.pluginProp("schema"),
                             PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MSSQL TARGET Table - " +
                                   PluginPropertyUtils.pluginProp("targetTable") + " deleted successfully");
  }

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
    String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-",
                                                                                                   "_");

    BeforeActions.scenario.write("Creating test data for Source BigQuery Table");
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

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTable() {
    String bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("bqtarget.table", bqTargetTable);
    BeforeActions.scenario.write("BigQuery Target table name: " + bqTargetTable);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTable = PluginPropertyUtils.pluginProp("bqtarget.table");
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
}

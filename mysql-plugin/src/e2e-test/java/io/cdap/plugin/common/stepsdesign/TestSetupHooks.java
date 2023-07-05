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
import io.cdap.plugin.MysqlClient;
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
 * MYSQL test hooks.
 */
public class TestSetupHooks {
  public static String bqTargetTable = StringUtils.EMPTY;

  private static void setTableName() {
    String randomString = RandomStringUtils.randomAlphabetic(10);
    String sourceTableName = String.format("SourceTable_%s", randomString);
    String targetTableName = String.format("TargetTable_%s", randomString);
    PluginPropertyUtils.addPluginProp("sourceTable", sourceTableName);
    PluginPropertyUtils.addPluginProp("targetTable", targetTableName);
    PluginPropertyUtils.addPluginProp("selectQuery", String.format("select * from %s", sourceTableName));
  }

  @Before(order = 1)
  public static void initializeDBProperties() {
    String username = System.getenv("username");
    if (username != null && !username.isEmpty()) {
        PluginPropertyUtils.addPluginProp("username", username);
    }
    String password = System.getenv("password");
    if (password != null && !password.isEmpty()) {
      PluginPropertyUtils.addPluginProp("password", password);
    }
    TestSetupHooks.setTableName();
  }

  @Before(order = 2, value = "@MYSQL_SOURCE_TEST")
  public static void createSourceTables() throws SQLException, ClassNotFoundException {
    MysqlClient.createSourceTable(PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MYSQL Source table - " + PluginPropertyUtils.pluginProp("sourceTable")
                                   + " created successfully");
  }

  @Before(order = 2, value = "@MYSQL_TARGET_TEST")
  public static void createTargetTables() throws SQLException, ClassNotFoundException {
    MysqlClient.createTargetTable(PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MYSQL Target table - " + PluginPropertyUtils.pluginProp("targetTable")
                                   + " created successfully");
  }

  @Before(order = 2, value = "@MYSQL_SOURCE_DATATYPES_TEST")
  public static void createSourceDatatypesTable() throws SQLException, ClassNotFoundException {
    MysqlClient.createSourceDatatypesTable(PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MYSQL Source Table - " + PluginPropertyUtils.pluginProp("sourceTable") +
                                   " created successfully");
  }

  @Before(order = 2, value = "@MYSQL_TARGET_DATATYPES_TEST")
  public static void createTargetDatatypesTable() throws SQLException, ClassNotFoundException {
    MysqlClient.createTargetDatatypesTable(PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MYSQL Target table - " + PluginPropertyUtils.pluginProp("targetTable")
                                   + " created successfully");
  }

  @After(order = 2, value = "@MYSQL_SOURCE_TEST")
  public static void dropSourceTables() throws SQLException, ClassNotFoundException {
    MysqlClient.dropTable(PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MYSQL Source Table - " + PluginPropertyUtils.pluginProp("sourceTable") +
                                   " deleted successfully");
  }

  @After(order = 2, value = "@MYSQL_TARGET_TEST")
  public static void dropTargetTables() throws SQLException, ClassNotFoundException {
    MysqlClient.dropTable(PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MYSQL Target table - " + PluginPropertyUtils.pluginProp("targetTable")
                                   + " deleted successfully");
  }

  @After(order = 2, value = "@MYSQL_SOURCE_DATATYPES_TEST")
  public static void dropSourceDatatypeTables() throws SQLException, ClassNotFoundException {
    MysqlClient.dropTable(PluginPropertyUtils.pluginProp("sourceTable"));
    BeforeActions.scenario.write("MYSQL Source Table - " + PluginPropertyUtils.pluginProp("sourceTable") +
                                   " deleted successfully");
  }

  @After(order = 2, value = "@MYSQL_TARGET_DATATYPES_TEST")
  public static void dropTargetDatatypeTables() throws SQLException, ClassNotFoundException {
    MysqlClient.dropTable(PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MYSQL Target table - " + PluginPropertyUtils.pluginProp("targetTable")
                                   + " deleted successfully");
  }

  @Before(order = 1, value = "@BQ_SINK")
  public static void setTempTargetBQTable() {
    bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTable);
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

  @Before(order = 2, value = "@MYSQL_TARGET_TABLE")
  public static void createMysqlTargetTable() throws SQLException, ClassNotFoundException {
    MysqlClient.createTargetTable1(PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MYSQL Target table - " + PluginPropertyUtils.pluginProp("targetTable")
                                   + " created successfully");
  }

  @After(order = 2, value = "@MYSQL_TARGET_TABLE")
  public static void dropTargetTable() throws SQLException, ClassNotFoundException {
    MysqlClient.dropTable(PluginPropertyUtils.pluginProp("targetTable"));
    BeforeActions.scenario.write("MYSQL Target table - " + PluginPropertyUtils.pluginProp("targetTable")
                                   + " deleted successfully");
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
  private static void createSourceBQTableWithQueries(String bqCreateTableQueryFile, String bqInsertDataQueryFile)
          throws IOException, InterruptedException {
    String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");

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
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException.
    }
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@CONNECTION")
  public static void setNewConnectionName() {
    String connectionName = "Mysql" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("connection.name", connectionName);
    BeforeActions.scenario.write("New Connection name: " + connectionName);
  }
}


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
package io.cdap.plugin;

import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;

import java.sql.*;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class CloudMySqlClient {

  public static Connection getCloudSqlConnection() throws ClassNotFoundException, SQLException {
    Class.forName("com.google.cloud.sql.mysql.SocketFactory");
    String instanceConnectionName = System.getenv("CLOUDSQLMYSQL_CONNECTIONNAME");
    String database = PluginPropertyUtils.pluginProp("DatabaseName");
    String username = System.getenv("CLOUDSQLMYSQL_USERNAME");
    String password = System.getenv("CLOUDSQLMYSQL_PASSWORD");
    String jdbcUrl = String.format(PluginPropertyUtils.pluginProp("jdbcURL"), database, instanceConnectionName,
                                   username, password);
    Connection conn = DriverManager.getConnection(jdbcUrl);
    return conn;
  }

  public static int countRecord(String table) throws SQLException, ClassNotFoundException {
    String countQuery = "SELECT COUNT(*) as total FROM " + table;
    try (Connection connect = getCloudSqlConnection(); Statement statement = connect.createStatement();
         ResultSet rs = statement.executeQuery(countQuery)) {
      int num = 0;
      while (rs.next()) {
        num = (rs.getInt(1));
      }
      return num;
    }
  }

  public static boolean validateRecordValues(String sourceTable, String targetTable) throws SQLException,
    ClassNotFoundException {
    String getSourceQuery = "SELECT * FROM " + sourceTable;
    String getTargetQuery = "SELECT * FROM " + targetTable;
    try (Connection connect = getCloudSqlConnection()) {
      connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement2 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);
      ResultSet rsSource = statement1.executeQuery(getSourceQuery);
      ResultSet rsTarget = statement2.executeQuery(getTargetQuery);
      return compareResultSetData(rsSource, rsTarget);
    }
  }

  /**
   * Compares the result Set data in source table and sink table.
   *
   * @param rsSource result set of the source table data
   * @param rsTarget result set of the target table data
   * @return true if rsSource matches rsTarget
   */
  public static boolean compareResultSetData(ResultSet rsSource, ResultSet rsTarget) throws SQLException {
    ResultSetMetaData mdSource = rsSource.getMetaData();
    ResultSetMetaData mdTarget = rsTarget.getMetaData();
    int columnCountSource = mdSource.getColumnCount();
    int columnCountTarget = mdTarget.getColumnCount();
    Assert.assertEquals("Number of columns in source and target are not equal",
                        columnCountSource, columnCountTarget);
    while (rsSource.next() && rsTarget.next()) {
      int currentColumnCount = 1;
      while (currentColumnCount <= columnCountSource) {
        String columnTypeName = mdSource.getColumnTypeName(currentColumnCount);
        int columnType = mdSource.getColumnType(currentColumnCount);
        String columnName = mdSource.getColumnName(currentColumnCount);
        if (columnType == Types.TIMESTAMP) {
          GregorianCalendar gc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
          gc.setGregorianChange(new Date(Long.MIN_VALUE));
          Timestamp sourceTS = rsSource.getTimestamp(currentColumnCount, gc);
          Timestamp targetTS = rsTarget.getTimestamp(currentColumnCount, gc);
          Assert.assertTrue(String.format("Different values found for column : %s", columnName),
                            sourceTS.equals(targetTS));
        } else {
          String sourceString = rsSource.getString(currentColumnCount);
          String targetString = rsTarget.getString(currentColumnCount);
          Assert.assertEquals(String.format("Different values found for column : %s", columnName),
                              String.valueOf(sourceString), String.valueOf(targetString));
        }
        currentColumnCount++;
      }
    }
    Assert.assertFalse("Number of rows in Source table is greater than the number of rows in Target table",
                       rsSource.next());
    Assert.assertFalse("Number of rows in Target table is greater than the number of rows in Source table",
                       rsTarget.next());
    return true;
  }

  public static void createSourceTable(String sourceTable) throws SQLException, ClassNotFoundException {
    try (Connection connection = getCloudSqlConnection();
         Statement statement = connection.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createSourceTableQuery = "CREATE TABLE " + sourceTable + datatypesColumns;
      statement.executeUpdate(createSourceTableQuery);
      System.out.println(createSourceTableQuery);
      // Insert dummy data.
      String datatypesValues = PluginPropertyUtils.pluginProp("datatypesValue1");
      String datatypesColumnsList = PluginPropertyUtils.pluginProp("datatypesColumnsList");
      statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList + " " +
                                datatypesValues);
    }
  }

  public static void createTargetTable(String targetTable) throws SQLException, ClassNotFoundException {
    try (Connection connection = getCloudSqlConnection();
         Statement statement = connection.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createTargetTableQuery = "CREATE TABLE " + targetTable + " " + datatypesColumns;
      statement.executeUpdate(createTargetTableQuery);
    }
  }

  public static void createSourceDatatypesTable(String sourceTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getCloudSqlConnection(); Statement statement = connect.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createSourceTableQuery = "CREATE TABLE " + sourceTable + " " + datatypesColumns;
      statement.executeUpdate(createSourceTableQuery);
      // Insert dummy data.
      String datatypesValues = PluginPropertyUtils.pluginProp("datatypesValue1");
      String datatypesColumnsList = PluginPropertyUtils.pluginProp("datatypesColumnsList");
      statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList + " " + datatypesValues);
    }
  }

  public static void createTargetDatatypesTable(String targetTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getCloudSqlConnection(); Statement statement = connect.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createTargetTableQuery = "CREATE TABLE " + targetTable + " " + datatypesColumns;
      statement.executeUpdate(createTargetTableQuery);
    }
  }

  public static void createTargetCloudMysqlTable(String targetTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getCloudSqlConnection(); Statement statement = connect.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("CloudMySqlDatatypesColumns");
      String createTargetTableQuery = "CREATE TABLE " + targetTable + " " + datatypesColumns;
      statement.executeUpdate(createTargetTableQuery);
    }
  }

  public static void dropTables(String[] tables) throws SQLException, ClassNotFoundException {
    try (Connection connect = getCloudSqlConnection(); Statement statement = connect.createStatement()) {
      for (String table : tables) {
        String dropTableQuery = "Drop Table " + table;
        statement.executeUpdate(dropTableQuery);
      }
    }
  }
}
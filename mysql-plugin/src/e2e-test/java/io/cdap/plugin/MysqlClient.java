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

package io.cdap.plugin;

import com.google.common.base.Strings;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * MySQL client.
 */
public class MysqlClient {
  private static final String database = PluginPropertyUtils.pluginProp("databaseName");

  public static Connection getMysqlConnection() throws SQLException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");
    return DriverManager.getConnection("jdbc:mysql://" + System.getenv("MYSQL_HOST") + ":" +
                                         System.getenv("MYSQL_PORT") + "/" + database + "?tinyInt1isBit=false",
                                       System.getenv("MYSQL_USERNAME"), System.getenv("MYSQL_PASSWORD"));
  }

  public static int countRecord(String table) throws SQLException, ClassNotFoundException {
    String countQuery = "SELECT COUNT(*) as total FROM " + table;
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement();
         ResultSet rs = statement.executeQuery(countQuery)) {
      int num = 0;
      while (rs.next()) {
        num = (rs.getInt(1));
      }
      return num;
    }
  }

  /**
   * Extracts entire data from source and target tables.
   *
   * @param sourceTable table at the source side
   * @param targetTable table at the sink side
   * @return true if the values in source and target side are equal
   */
  public static boolean validateRecordValues(String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException {
    String getSourceQuery = "SELECT * FROM " + sourceTable;
    String getTargetQuery = "SELECT * FROM " + targetTable;
    try (Connection connect = getMysqlConnection()) {
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
   * Compares the result Set data in source table and sink table..
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
          if (sourceTS == null) {
            Assert.assertNull(String.format("Not null TIMESTAMP value found in target for column : %s", columnName),
                              targetTS);
          } else {
            Assert.assertEquals(String.format("Different TIMESTAMP values found for column : %s", columnName),
                                sourceTS, targetTS);
          }
        } else {
          String sourceString = rsSource.getString(currentColumnCount);
          String targetString = rsTarget.getString(currentColumnCount);
          if (Strings.isNullOrEmpty(sourceString)) {
            Assert.assertTrue(String.format("Not null/empty value found in target for column : %s", columnName),
                              Strings.isNullOrEmpty(targetString));
          } else {
            Assert.assertEquals(String.format("Different values found for column : %s", columnName),
                                sourceString, targetString);
          }
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
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String createSourceTableQuery = "CREATE TABLE IF NOT EXISTS " + sourceTable +
        "(id int, lastName varchar(255), PRIMARY KEY (id))";
      statement.executeUpdate(createSourceTableQuery);

      // Truncate table to clean the data of last failure run.
      String truncateSourceTableQuery = "TRUNCATE TABLE " + sourceTable;
      statement.executeUpdate(truncateSourceTableQuery);

      // Insert dummy data.
      statement.executeUpdate("INSERT INTO " + sourceTable + " (id, lastName)" +
                                "VALUES (1, 'Simpson')");
      statement.executeUpdate("INSERT INTO " + sourceTable + " (id, lastName)" +
                                "VALUES (2, 'McBeal')");
      statement.executeUpdate("INSERT INTO " + sourceTable + " (id, lastName)" +
                                "VALUES (3, 'Flinstone')");
    }
  }

  public static void createTargetTable(String targetTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String createTargetTableQuery = "CREATE TABLE IF NOT EXISTS " + targetTable +
        "(id int, lastName varchar(255), PRIMARY KEY (id))";
      statement.executeUpdate(createTargetTableQuery);
      // Truncate table to clean the data of last failure run.
      String truncateTargetTableQuery = "TRUNCATE TABLE " + targetTable;
      statement.executeUpdate(truncateTargetTableQuery);
    }
  }

  public static void createTargetTable1(String targetTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String createTargetTableQuery = "CREATE TABLE IF NOT EXISTS " + targetTable +
        "(id bigint(20),lastName varchar(255))";
      statement.executeUpdate(createTargetTableQuery);
      // Truncate table to clean the data of last failure run.
      String truncateTargetTableQuery = "TRUNCATE TABLE " + targetTable;
      statement.executeUpdate(truncateTargetTableQuery);
    }
  }

  public static void createSourceDatatypesTable(String sourceTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createSourceTableQuery = "CREATE TABLE " + sourceTable + " " + datatypesColumns;
      statement.executeUpdate(createSourceTableQuery);

      // Insert dummy data.
      int rowCount = 1;
      while (!Strings.isNullOrEmpty(PluginPropertyUtils.pluginProp("datatypesValue" + rowCount))) {
        String datatypesValues = PluginPropertyUtils.pluginProp("datatypesValue" + rowCount);
        String datatypesColumnsList = PluginPropertyUtils.pluginProp("datatypesColumnsList");
        statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList + " " + datatypesValues);
        rowCount++;
      }
    }
  }

  public static void createTargetDatatypesTable(String targetTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createTargetTableQuery = "CREATE TABLE " + targetTable + " " + datatypesColumns;
      statement.executeUpdate(createTargetTableQuery);
    }
  }

  public static void dropTables(String[] tables) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      for (String table : tables) {
        String dropTableQuery = "Drop Table " + table;
        statement.executeUpdate(dropTableQuery);
      }
    }
  }

  public static void dropTable(String table) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String dropTableQuery = "Drop Table " + table;
      statement.executeUpdate(dropTableQuery);
    }
  }
}


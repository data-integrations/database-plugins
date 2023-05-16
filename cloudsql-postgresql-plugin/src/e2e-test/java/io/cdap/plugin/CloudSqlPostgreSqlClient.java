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
 * CloudSQLPostgreSQL client.
 */

public class CloudSqlPostgreSqlClient {

  private static final String DATA_TYPES_COLUMNS = PluginPropertyUtils.pluginProp("datatypesColumns");
  private static final String DATA_TYPES_VALUES = PluginPropertyUtils.pluginProp("datatypesValues");
  private static final String DATA_TYPES_COLUMNS_LIST = PluginPropertyUtils.pluginProp("datatypesColumnsList");
  private static final String BIG_QUERY_DATATYPE_COLUMNS = PluginPropertyUtils.pluginProp("bigQueryDatatypesColumns");
  public static Connection getCloudSqlConnection() throws ClassNotFoundException, SQLException {
    Class.forName("org.postgresql.Driver");
    String database = PluginPropertyUtils.pluginProp("databaseName");
    String instanceConnectionName = System.getenv("CLOUDSQL_POSTGRESQL_CONNECTION_NAME");
    String username = System.getenv("CLOUDSQL_POSTGRESQL_USERNAME");
    String password = System.getenv("CLOUDSQL_POSTGRESQL_PASSWORD");
    String jdbcUrl = String.format(PluginPropertyUtils.pluginProp("URL"), database, instanceConnectionName,
                                   username, password);
    Connection connection = DriverManager.getConnection(jdbcUrl);
    return connection;
  }

  public static int countRecord(String table, String schema) throws SQLException, ClassNotFoundException {
    String countQuery = "SELECT COUNT(*) as total FROM " + schema + "." + table;
    try (Connection connection = getCloudSqlConnection();
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(countQuery)) {
      int num = 0;
      while (rs.next()) {
        num = (rs.getInt(1));
      }
      return num;
    }
  }

  /**
   * Extracts Result set of source and target table.
   *
   * @param sourceTable table at the source side
   * @param targetTable table at the sink side
   * @return true if the values in source and target side are equal
   */
  public static boolean validateRecordValues(String sourceTable, String targetTable, String schema)
    throws SQLException, ClassNotFoundException {
    String getSourceQuery = "SELECT * FROM " + schema + "." + sourceTable;
    String getTargetQuery = "SELECT * FROM " + schema + "." + targetTable;
    try (Connection connection = getCloudSqlConnection()) {
      connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement2 = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
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
          Assert.assertEquals(String.format("Different values found for column : %s", columnName), sourceTS, targetTS);
        } else {
          String sourceString = rsSource.getString(currentColumnCount);
          String targetString = rsTarget.getString(currentColumnCount);
          Assert.assertEquals(String.format("Different values found for column : %s", columnName),
                              sourceString, targetString);
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

  public static void createSourceTable(String sourceTable, String schema) throws SQLException, ClassNotFoundException {
    try (Connection connection = getCloudSqlConnection();
         Statement statement = connection.createStatement()) {
      String datatypesColumns = DATA_TYPES_COLUMNS;
      String createSourceTableQuery = "CREATE TABLE " + schema + "." + sourceTable + datatypesColumns;
      statement.executeUpdate(createSourceTableQuery);

      // Insert dummy data.
      String datatypesValues = DATA_TYPES_VALUES;
      String datatypesColumnsList = DATA_TYPES_COLUMNS_LIST;
      statement.executeUpdate("INSERT INTO " + schema + "." + sourceTable + " " + datatypesColumnsList + " " +
                                datatypesValues);
    }
  }

  public static void createTargetTable(String targetTable, String schema) throws SQLException, ClassNotFoundException {
    try (Connection connection = getCloudSqlConnection();
         Statement statement = connection.createStatement()) {
      String datatypesColumns = DATA_TYPES_COLUMNS;
      String createTargetTableQuery = "CREATE TABLE " + schema + "." + targetTable + " " + datatypesColumns;
      statement.executeUpdate(createTargetTableQuery);
    }
  }

  public static void createTargetPostgresqlTable(String targetTable, String schema) throws SQLException,
    ClassNotFoundException {
    try (Connection connection = getCloudSqlConnection();
         Statement statement = connection.createStatement()) {
      String datatypesColumns = BIG_QUERY_DATATYPE_COLUMNS;
      String createTargetTableQuery = "CREATE TABLE " + schema + "." + targetTable + " " + datatypesColumns;
      statement.executeUpdate(createTargetTableQuery);
    }
  }

  public static void deleteTable(String table, String schema) throws SQLException, ClassNotFoundException {
    try (Connection connection = getCloudSqlConnection();
         Statement statement = connection.createStatement()) {
        String dropTableQuery = "Drop Table " + schema + "." + table;
        statement.executeUpdate(dropTableQuery);
      }
  }
}

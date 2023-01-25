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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

/**
 *  Oracle client.
 */
public class OracleClient {

  private static Connection getOracleConnection() throws SQLException, ClassNotFoundException {
    TimeZone timezone = TimeZone.getTimeZone("UTC");
    TimeZone.setDefault(timezone);
    Class.forName("oracle.jdbc.driver.OracleDriver");
    String databaseName = PluginPropertyUtils.pluginProp("databaseName");
    return DriverManager.getConnection("jdbc:oracle:thin:@//" + System.getenv("ORACLE_HOST")
                                         + ":" + System.getenv("ORACLE_PORT") + "/" + databaseName,
                                       System.getenv("ORACLE_USERNAME"), System.getenv("ORACLE_PASSWORD"));
  }

  public static int countRecord(String table, String schema) throws SQLException, ClassNotFoundException {
    String countQuery = "SELECT COUNT(*) as total FROM " + schema + "." + table;
    try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement();
         ResultSet rs = statement.executeQuery(countQuery)) {
      int num = 0;
      while (rs.next()) {
        num = (rs.getInt(1));
      }
      return num;
    }
  }

  public static void createSourceTable(String sourceTable, String schema) throws SQLException, ClassNotFoundException {
    try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
      String createSourceTableQuery = "CREATE TABLE " + schema + "." + sourceTable +
        "(ID number(38), LASTNAME varchar2(100))";
      statement.executeUpdate(createSourceTableQuery);

      // Insert dummy data.
      statement.executeUpdate("INSERT INTO " + schema + "." + sourceTable + " (ID, LASTNAME)" +
                                " VALUES (1, 'Shelby')");
      statement.executeUpdate("INSERT INTO " + schema + "." + sourceTable + " (ID, LASTNAME)" +
                                " VALUES (2, 'Simpson')");
    }
  }

  public static void createTargetTable(String targetTable, String schema) throws SQLException, ClassNotFoundException {
    try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
      String createTargetTableQuery = "CREATE TABLE " + schema + "." + targetTable +
        "(ID number(38), LASTNAME varchar2(100))";
      statement.executeUpdate(createTargetTableQuery);
    }
  }

  public static void deleteTables(String schema, String[] tables)
    throws SQLException, ClassNotFoundException {
    try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
      for (String table : tables) {
        String dropTableQuery = "DROP TABLE " + schema + "." + table;
        statement.execute(dropTableQuery);
      }
    }
  }
}

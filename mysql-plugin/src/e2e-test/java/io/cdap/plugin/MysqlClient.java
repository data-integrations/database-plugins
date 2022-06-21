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

import io.cdap.e2e.utils.PluginPropertyUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *  MySQL client
 */
public class MysqlClient {
  private static final String host =  PluginPropertyUtils.pluginProp("host");
  private static final int port = Integer.parseInt(PluginPropertyUtils.pluginProp("port"));
  private static final String database = PluginPropertyUtils.pluginProp("database");

  private static Connection getMysqlConnection() throws SQLException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");
    String username = PluginPropertyUtils.pluginProp("username");
    String password = PluginPropertyUtils.pluginProp("password");
    return DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database, username, password);
  }

  public static int countRecord(String table) throws SQLException, ClassNotFoundException {
    String countQuery = "SELECT COUNT(*) as total FROM " + table;
    try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement();
         ResultSet rs = statement.executeQuery(countQuery)) {
      int num = 0;
      while(rs.next()){
        num = (rs.getInt(1));
      }
      return num;
    }
  }

  public static void createSourceTable(String sourceTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
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
    try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
      String createTargetTableQuery = "CREATE TABLE IF NOT EXISTS " + targetTable +
        "(id int, lastName varchar(255), PRIMARY KEY (id))";
      statement.executeUpdate(createTargetTableQuery);
      // Truncate table to clean the data of last failure run.
      String truncateTargetTableQuery = "TRUNCATE TABLE " + targetTable;
      statement.executeUpdate(truncateTargetTableQuery);
    }
  }

  public static void dropTables(String[] tables) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
      for (String table : tables) {
        String dropTableQuery = "Drop Table " + table;
        statement.executeUpdate(dropTableQuery);
      }
    }
  }
}

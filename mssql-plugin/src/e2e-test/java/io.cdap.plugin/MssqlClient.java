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
 *  Mssql client.
 */
public class MssqlClient {

    public static Connection getMssqlConnection() throws SQLException, ClassNotFoundException {
        TimeZone timezone = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(timezone);
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String databaseName = PluginPropertyUtils.pluginProp("databaseName");
        return DriverManager.getConnection("jdbc:sqlserver://" + System.getenv("MSSQL_HOST")
                        + ":" + System.getenv("MSSQL_PORT") + ";databaseName=" + databaseName,
                System.getenv("MSSQL_USERNAME"), System.getenv("MSSQL_PASSWORD"));
    }

    public static int countRecord(String table, String schema) throws SQLException, ClassNotFoundException {
        String countQuery = "SELECT COUNT(*) as total FROM " + schema + "." + table;
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement();
             ResultSet rs = statement.executeQuery(countQuery)) {
            int num = 0;
            while (rs.next()) {
                num = (rs.getInt(1));
            }
            return num;
        }
    }

    public static void createSourceTable(String sourceTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String createSourceTableQuery = createTableQuery(sourceTable, schema,
                    "(ID varchar(100), LASTNAME varchar(100))");
            statement.executeUpdate(createSourceTableQuery);

            // Insert dummy data.
            statement.executeUpdate("INSERT INTO " + schema + "." + sourceTable + " (ID, LASTNAME)" +
                    " VALUES ('id1', 'Shelby')");
            statement.executeUpdate("INSERT INTO " + schema + "." + sourceTable + " (ID, LASTNAME)" +
                    " VALUES ('id2', 'Simpson')");
        }
    }

    public static void createTargetTable(String targetTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String createTargetTableQuery = createTableQuery(targetTable, schema,
                    "(ID varchar(100), LASTNAME varchar(100))");
            statement.executeUpdate(createTargetTableQuery);
        }
    }

    public static void createSourceDatatypesTable(String sourceTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String datatypeColumns = PluginPropertyUtils.pluginProp("datatypeColumns");
            String createSourceTableQuery2 = createTableQuery(sourceTable, schema, datatypeColumns);
            statement.executeUpdate(createSourceTableQuery2);

            // Insert dummy data.
            String datatypeValues = PluginPropertyUtils.pluginProp("datatypeValues");
            String datatypeColumnsList = PluginPropertyUtils.pluginProp("datatypeColumnsList");
            statement.executeUpdate(insertQuery(sourceTable, schema, datatypeColumnsList, datatypeValues));
        }
    }

    public static void createTargetDatatypesTable(String targetTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String datatypeColumns = PluginPropertyUtils.pluginProp("datatypeColumns");
            String createTargetTableQuery2 = createTableQuery(targetTable, schema, datatypeColumns);
            statement.executeUpdate(createTargetTableQuery2);
        }
    }

    public static void createSourceImageTable(String sourceTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String imageColumns = PluginPropertyUtils.pluginProp("imageColumns");
            String createSourceTableQuery3 = createTableQuery(sourceTable, schema, imageColumns);
            statement.executeUpdate(createSourceTableQuery3);

            // Insert dummy data.
            String imageValues = PluginPropertyUtils.pluginProp("imageValues");
            String imageColumnsList = PluginPropertyUtils.pluginProp("imageColumnsList");
            statement.executeUpdate(insertQuery(sourceTable, schema, imageColumnsList, imageValues));
        }
    }

    public static void createTargetImageTable(String targetTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String imageColumns = PluginPropertyUtils.pluginProp("imageColumns");
            String createTargetTableQuery3 = createTableQuery(targetTable, schema, imageColumns);
            statement.executeUpdate(createTargetTableQuery3);
        }
    }

    public static void createSourceUniqueIdentifierTable(String sourceTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String uniqueIdentifierColumns = PluginPropertyUtils.pluginProp("uniqueIdentifierColumns");
            String createSourceTableQuery3 = createTableQuery(sourceTable, schema, uniqueIdentifierColumns);
            statement.executeUpdate(createSourceTableQuery3);

            // Insert dummy data.
            String uniqueIdentifierValues = PluginPropertyUtils.pluginProp("uniqueIdentifierValues");
            String uniqueIdentifierColumnsList = PluginPropertyUtils.pluginProp("uniqueIdentifierColumnsList");
            statement.executeUpdate(insertQuery(sourceTable, schema, uniqueIdentifierColumnsList,
                    uniqueIdentifierValues));
        }
    }
    public static void createTargetMssqlTable(String targetTable, String schema) throws SQLException,
      ClassNotFoundException {
        try (Connection connect = getMssqlConnection();
             Statement statement = connect.createStatement()) {
            String datatypesColumns = PluginPropertyUtils.pluginProp("SqlServerDatatypesColumns");
            String createSourceTableQuery2 = createTableQuery(targetTable, schema, datatypesColumns);
            statement.executeUpdate(createSourceTableQuery2);
        }
    }

    public static void createTargetUniqueIdentifierTable(String targetTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String uniqueIdentifierColumns = PluginPropertyUtils.pluginProp("uniqueIdentifierColumns");
            String createTargetTableQuery3 = createTableQuery(targetTable, schema, uniqueIdentifierColumns);
            statement.executeUpdate(createTargetTableQuery3);
        }
    }

    public static void createSourceDateTimeTable(String sourceTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String dateTimeColumns = PluginPropertyUtils.pluginProp("dateTimeColumns");
            String createSourceTableQuery3 = createTableQuery(sourceTable, schema, dateTimeColumns);
            statement.executeUpdate(createSourceTableQuery3);

            // Insert dummy data.
            String dateTimeValues = PluginPropertyUtils.pluginProp("dateTimeValues");
            String dateTimeColumnsList = PluginPropertyUtils.pluginProp("dateTimeColumnsList");
            statement.executeUpdate(insertQuery(sourceTable, schema, dateTimeColumnsList,
                    dateTimeValues));
        }
    }

    public static void createTargetDateTimeTable(String targetTable, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String dateTimeColumns = PluginPropertyUtils.pluginProp("dateTimeColumns");
            String createTargetTableQuery3 = createTableQuery(targetTable, schema, dateTimeColumns);
            statement.executeUpdate(createTargetTableQuery3);
        }
    }

    public static void deleteTable(String schema, String table)
      throws SQLException, ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
                String dropTableQuery = "DROP TABLE " + schema + "." + table;
                statement.execute(dropTableQuery);
        }
    }

    public static boolean validateRecordValues(String schema, String sourceTable, String targetTable)
            throws SQLException, ClassNotFoundException {
        String getSourceQuery = "SELECT * FROM " + schema + "." + sourceTable;
        String getTargetQuery = "SELECT * FROM " + schema + "." + targetTable;
        try (Connection connect = getMssqlConnection()) {
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

    private static String createTableQuery(String table, String schema, String columns) {
        return String.format("CREATE TABLE %s.%s %s", schema, table, columns);
    }

    private static String insertQuery(String table, String schema, String columnList, String columnValues) {
        return String.format("INSERT INTO %s.%s %s %s", schema, table,
                columnList, columnValues);
    }

    private static boolean compareResultSetData(ResultSet rsSource, ResultSet rsTarget) throws SQLException {
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
                if (columnType == Types.TIMESTAMP || columnTypeName.equalsIgnoreCase("datetimeoffset")) {
                    GregorianCalendar gc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                    gc.setGregorianChange(new Date(Long.MIN_VALUE));
                    Timestamp sourceTS = rsSource.getTimestamp(currentColumnCount, gc);
                    Timestamp targetTS = rsTarget.getTimestamp(currentColumnCount, gc);
                    Assert.assertEquals(String.format("Different values found for column : %s", columnName),
                            sourceTS, targetTS);

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
}


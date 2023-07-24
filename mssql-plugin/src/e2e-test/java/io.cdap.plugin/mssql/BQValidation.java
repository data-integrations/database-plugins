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
package io.cdap.plugin.mssql;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.MssqlClient;
import org.apache.spark.sql.types.Decimal;
import org.junit.Assert;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.TimeZone;

/**
 * BQValidation.
 */
public class BQValidation {
  /**
   * Extracts entire data from source and target tables.
   *
   * @param sourceTable table at the source side
   * @param targetTable table at the sink side
   * @return true if the values in source and target side are equal
   */

  public static boolean validateDBToBQRecordValues(String schema, String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, IOException, InterruptedException, ParseException {
    List<Object> targetBigQueryRows = new ArrayList<>();
    List<JsonObject> targetBigQueryResponse = new ArrayList<>();

    getBigQueryTableData(targetTable, targetBigQueryRows);

    for (Object rows : targetBigQueryRows) {
      JsonObject json = new Gson().fromJson(String.valueOf(rows), JsonObject.class);
      targetBigQueryResponse.add(json);
    }

    String getSourceQuery = "SELECT * FROM " + schema + "." + sourceTable;
    try (Connection connect = MssqlClient.getMssqlConnection();
         ResultSet rsSource = executeQuery(connect, getSourceQuery)) {
      return compareResultSetAndJsonData(rsSource, targetBigQueryResponse);
    }
  }

  public static boolean validateBQToDBRecordValues(String schema, String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, IOException, InterruptedException, ParseException {
    List<Object> sourceBigQueryRows = new ArrayList<>();
    List<JsonObject> sourceBigQueryResponse = new ArrayList<>();

    getBigQueryTableData(sourceTable, sourceBigQueryRows);
    for (Object rows : sourceBigQueryRows) {
      JsonObject json = new Gson().fromJson(String.valueOf(rows), JsonObject.class);
      sourceBigQueryResponse.add(json);
    }
    String getTargetQuery = "SELECT * FROM " + schema + "." + targetTable;
    try (Connection connect = MssqlClient.getMssqlConnection();
         ResultSet rsSource = executeQuery(connect, getTargetQuery)) {
      return compareResultSetAndJsonData(rsSource, sourceBigQueryResponse);
    }
  }

  /**
   * Executes the given SQL query on the provided database connection and returns the result set.
   *
   * @param connect The Connection object representing the active database connection.
   * @param query   The SQL query to be executed on the database.
   * @return A ResultSet object containing the data retrieved by the executed query.
   * @throws SQLException If a database access error occurs or the SQL query is invalid.
   */

  public static ResultSet executeQuery(Connection connect, String query) throws SQLException {
    connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    Statement statement = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);
    return statement.executeQuery(query);
    }

  /**
   * Retrieves the data from a specified BigQuery table and populates it into the provided list of objects.
   *
   * @param table        The name of the BigQuery table to fetch data from.
   * @param bigQueryRows The list to store the fetched BigQuery data.
   */
  private static void getBigQueryTableData(String table, List<Object> bigQueryRows)
    throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);

    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));
  }

  /**
   * Compares the data in the result set obtained from the Oracle database with the provided BigQuery JSON objects.
   *
   * @param rsSource     The result set obtained from the Oracle database.
   * @param bigQueryData The list of BigQuery JSON objects to compare with the result set data.
   * @return True if the result set data matches the BigQuery data, false otherwise.
   * @throws SQLException   If an SQL error occurs during the result set operations.
   * @throws ParseException If an error occurs while parsing the data.
   */
  public static boolean compareResultSetAndJsonData(ResultSet rsSource, List<JsonObject> bigQueryData)
    throws SQLException, ParseException {
    ResultSetMetaData mdSource = rsSource.getMetaData();
    boolean result = false;
    int columnCountSource = mdSource.getColumnCount();

    if (bigQueryData == null) {
      Assert.fail("bigQueryData is null");
      return result;
    }

    //Variable 'jsonObjectIdx' to track the index of the current JsonObject in the bigQueryData list,
    int jsonObjectIdx = 0;
    int columnCountTarget = 0;

    if (bigQueryData.size() > 0) {
      columnCountTarget = bigQueryData.get(jsonObjectIdx).entrySet().size();
    }

    // Compare the number of columns in the source and target
    Assert.assertEquals(columnCountSource, columnCountTarget);
    while (rsSource.next()) {
      int currentColumnCount = 1;
      while (currentColumnCount <= columnCountSource) {
        String columnTypeName = mdSource.getColumnTypeName(currentColumnCount);
        int columnType = mdSource.getColumnType(currentColumnCount);
        String columnName = mdSource.getColumnName(currentColumnCount);
        switch (columnType) {
          case Types.BIT:
            boolean source = rsSource.getBoolean(currentColumnCount);
            boolean target = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBoolean();
            Assert.assertEquals("Different values found for column : %s", source, target);
            break;
          case Types.DECIMAL:
          case Types.NUMERIC:
            Decimal sourceDecimal = Decimal.fromDecimal(rsSource.getBigDecimal(currentColumnCount));
            Decimal targetDecimal = Decimal.fromDecimal(
              bigQueryData.get(jsonObjectIdx).get(columnName).getAsBigDecimal());
            Assert.assertEquals("Different values found for column : %s", sourceDecimal, targetDecimal);
            break;
          case Types.REAL:
            float sourceReal = rsSource.getFloat(currentColumnCount);
            float targetReal = bigQueryData.get(jsonObjectIdx).get(columnName).getAsFloat();
            Assert.assertEquals(String.format("Different values found for column : %s", columnName), 0,
                                Float.compare(sourceReal, targetReal));
            break;

          case Types.DOUBLE:
          case Types.FLOAT:
            double sourceVal = rsSource.getDouble(currentColumnCount);
            double targetVal = bigQueryData.get(jsonObjectIdx).get(columnName).getAsDouble();
            Assert.assertEquals(String.format("Different values found for column : %s", columnName), 0,
                                Double.compare(sourceVal, targetVal));
            break;
          case Types.TIME:
            Time sourceTime = rsSource.getTime(currentColumnCount);
            Time targetTime = Time.valueOf(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", sourceTime, targetTime);
            break;
          case Types.BINARY:
          case Types.VARBINARY:
          case SqlServerSourceSchemaReader.GEOMETRY_TYPE:
          case SqlServerSourceSchemaReader.GEOGRAPHY_TYPE:
            String sourceB64String = new String(Base64.getEncoder().encode(rsSource.getBytes(currentColumnCount)));
            String targetB64String = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals("Different values found for column : %s",
                                sourceB64String, targetB64String);
            break;
          case Types.BIGINT:
            long sourceInt = rsSource.getLong(currentColumnCount);
            long targetInt = bigQueryData.get(jsonObjectIdx).get(columnName).getAsLong();
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceInt),
                                String.valueOf(targetInt));
            break;
          case Types.SMALLINT:
          case Types.TINYINT:
          case Types.INTEGER:
            int sourceValue = rsSource.getInt(currentColumnCount);
            int targetValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsInt();
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceValue),
                                String.valueOf(targetValue));
            break;
          case Types.DATE:
            Date dateSource = rsSource.getDate(currentColumnCount);
            Date dateTarget = Date.valueOf(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", dateSource, dateTarget);
            break;
          case Types.TIMESTAMP:
            Timestamp timestampSource = rsSource.getTimestamp(currentColumnCount);
            String targetTimestampString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            java.util.Date parsedDate = dateFormat.parse(targetTimestampString);
            Timestamp timestampParsed = new Timestamp(parsedDate.getTime());
            Assert.assertEquals("Different Values found for column : %s", timestampSource,
                                timestampParsed);
            break;
          default:
          case Types.VARCHAR:
          case Types.CHAR:
          case Types.NCHAR:
          case Types.NVARCHAR:
          case Types.LONGNVARCHAR:
          case Types.OTHER:
            String sourceString = rsSource.getString(currentColumnCount);
            String targetString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals(String.format("Different %s values found for column : %s", columnTypeName,
                                              columnName),
                                String.valueOf(sourceString), String.valueOf(targetString));
        }
        currentColumnCount++;
      }
      jsonObjectIdx++;
    }
    Assert.assertFalse("Number of rows in Source table is greater than the number of rows in Target table",
                       rsSource.next());
    return true;
  }
}

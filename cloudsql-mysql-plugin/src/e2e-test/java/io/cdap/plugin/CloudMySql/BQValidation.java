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
package io.cdap.plugin.CloudMySql;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.CloudMySqlClient;
import org.junit.Assert;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.spark.sql.types.Decimal;

/**
 * BQValidation
 */

public class BQValidation {

  public static List<JsonObject> bigQueryResponse = new ArrayList<>();
  public static List<Object> bigQueryRows = new ArrayList<>();
  public static Gson gson = new Gson();

  /**
   * Extracts entire data from source and target tables.
   *
   * @param sourceTable table at the source side
   * @param targetTable table at the sink side
   * @return true if the values in source and target side are equal
   */

  public static boolean validateBQAndDBRecordValues(String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, IOException, InterruptedException, ParseException {
    getBigQueryTableData(sourceTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(json);
    }
    String getSourceQuery = "SELECT * FROM " + targetTable;
    try (Connection connect = CloudMySqlClient.getCloudSqlConnection()) {
      connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);
      ResultSet rsTarget = statement1.executeQuery(getSourceQuery);
      return compareResultSetWithJsonData(rsTarget, bigQueryResponse);
    }
  }

  public static boolean validateDBAndBQRecordValues(String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, IOException, InterruptedException, ParseException {
    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(json);
    }
    String getTargetQuery = "SELECT * FROM " + sourceTable;
    try (Connection connect = CloudMySqlClient.getCloudSqlConnection()) {
      connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);
      ResultSet rsSource = statement1.executeQuery(getTargetQuery);
      return compareResultSetWithJsonData(rsSource, bigQueryResponse);
    }
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
   * Compares the data in the result set obtained from the CloudSqlMySql database with provided BigQuery JSON objects.
   *
   * @param rsSource     The result set obtained from the CloudSql MySql database.
   * @param bigQueryData The list of BigQuery JSON objects to compare with the result set data.
   * @return True if the result set data matches the BigQuery data, false otherwise.
   * @throws SQLException If an SQL error occurs during the result set operations.
   */

  public static boolean compareResultSetWithJsonData(ResultSet rsSource, List<JsonObject> bigQueryData) throws
    SQLException {
    ResultSetMetaData mdSource = rsSource.getMetaData();
    boolean result = false;
    int columnCountSource = mdSource.getColumnCount();

    if (bigQueryData == null) {
      Assert.fail("bigQueryData is null");
      return result;
    }

    // Get the column count of the first JsonObject in bigQueryData
    int columnCountTarget = 0;
    if (bigQueryData.size() > 0) {
      columnCountTarget = bigQueryData.get(0).entrySet().size();
    }
    // Compare the number of columns in the source and target
    Assert.assertEquals("Number of columns in source and target are not equal",
                        columnCountSource, columnCountTarget);

    //Variable 'jsonObjectIdx' to track the index of the current JsonObject in the bigQueryData list,
    int jsonObjectIdx = 0;
    while (rsSource.next()) {
      int currentColumnCount = 2;
      while (currentColumnCount <= columnCountSource) {
        String columnTypeName = mdSource.getColumnTypeName(currentColumnCount);
        int columnType = mdSource.getColumnType(currentColumnCount);
        String columnName = mdSource.getColumnName(currentColumnCount);
        // Perform different comparisons based on column type
        switch (columnType) {
          // Since we skip BFILE in Oracle Sink, we are not comparing the BFILE source and sink values
          case Types.BIT:
            Boolean sourceBit = rsSource.getBoolean(currentColumnCount);
            Boolean targetBit = Boolean.parseBoolean(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceBit),
                                String.valueOf(targetBit));
            break;

          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.TINYINT:
            Integer sourceTinyInt = rsSource.getInt(currentColumnCount);
            Integer targetTinyInt = Integer.parseInt(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceTinyInt),
                                String.valueOf(targetTinyInt));
            break;

          case Types.REAL:
            Float sourceFloat = rsSource.getFloat(currentColumnCount);
            Float targetFloat = Float.parseFloat(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceFloat),
                                String.valueOf(targetFloat));
            break;

          case Types.DOUBLE:
            Double sourceDouble = rsSource.getDouble(currentColumnCount);
            Double targetDouble = Double.parseDouble(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceDouble),
                                String.valueOf(targetDouble));
            break;

          case Types.DATE:
            Date sourceDate = rsSource.getDate(currentColumnCount);
            Date targetDate = java.sql.Date.valueOf(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceDate),
                                String.valueOf(targetDate));
            break;

          case Types.TIME:
            Time sourceTime = rsSource.getTime(currentColumnCount);
            Time targetTime = Time.valueOf(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceTime),
                                String.valueOf(targetTime));
            break;

          case Types.DECIMAL:
            Decimal sourceDecimal = Decimal.fromDecimal(rsSource.getBigDecimal(currentColumnCount));
            Decimal targetDecimal = Decimal.fromDecimal(
              bigQueryData.get(jsonObjectIdx).get(columnName).getAsBigDecimal());
            Assert.assertEquals("Different values found for column : %s", sourceDecimal, targetDecimal);
            break;

          case Types.BLOB:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
          case Types.BINARY:
            String sourceB64String = new String(Base64.getEncoder().encode(rsSource.getBytes(currentColumnCount)));
            String targetB64String = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals("Different values found for column : %s",
                                sourceB64String, targetB64String);
            break;

          case Types.NUMERIC:
            long sourceVal = rsSource.getLong(currentColumnCount);
            long targetVal = Long.parseLong(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceVal),
                                String.valueOf(targetVal));
            break;

          case Types.TIMESTAMP:
            String sourceTS = String.valueOf(rsSource.getTimestamp(currentColumnCount));
            String targetTS = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            LocalDateTime timestamp = LocalDateTime.parse(targetTS, DateTimeFormatter.ISO_DATE_TIME);
            ZonedDateTime utcDateTime = ZonedDateTime.of(timestamp, ZoneOffset.UTC);
            ZoneId systemTimeZone = ZoneId.systemDefault();
            ZonedDateTime convertedDateTime = utcDateTime.withZoneSameInstant(systemTimeZone);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
            String formattedTimestamp = convertedDateTime.format(formatter);
            Assert.assertEquals(sourceTS, formattedTimestamp);
            break;

          default:
            String sourceString = rsSource.getString(currentColumnCount);
            String targetString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals(String.format("Different %s values found for column : %s", columnTypeName, columnName),
                                String.valueOf(sourceString), String.valueOf(targetString));
            break;
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

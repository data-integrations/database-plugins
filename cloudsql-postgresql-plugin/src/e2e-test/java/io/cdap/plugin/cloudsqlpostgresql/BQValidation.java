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
package io.cdap.plugin.cloudsqlpostgresql;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.CloudSqlPostgreSqlClient;
import org.apache.spark.sql.types.Decimal;
import org.junit.Assert;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

/**
 * BQValidation.
 */
public class BQValidation {
  static List<JsonObject> bigQueryResponse = new ArrayList<>();
  static List<Object> bigQueryRows = new ArrayList<>();
  static Gson gson = new Gson();

  /**
   * Extracts entire data from source and target tables.
   *
   * @param sourceTable table at the source side
   * @param targetTable table at the sink side
   * @return true if the values in source and target side are equal
   */
  public static boolean validateDBToBQRecordValues(String schema, String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, IOException, InterruptedException, ParseException {
    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(json);
    }
    String getSourceQuery = "SELECT * FROM " + schema + "." + sourceTable;
    try (Connection connection = CloudSqlPostgreSqlClient.getCloudSqlConnection()) {
      connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                        ResultSet.HOLD_CURSORS_OVER_COMMIT);

      ResultSet rsSource = statement1.executeQuery(getSourceQuery);
      return compareResultSetandJsonData(rsSource, bigQueryResponse);
    }
  }

  public static boolean validateBQToDBRecordValues(String schema, String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, IOException, InterruptedException, ParseException {
    getBigQueryTableData(sourceTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(json);
    }
    String getTargetQuery = "SELECT * FROM " + schema + "." + targetTable;
    try (Connection connection = CloudSqlPostgreSqlClient.getCloudSqlConnection()) {
      connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                        ResultSet.HOLD_CURSORS_OVER_COMMIT);

      ResultSet rsTarget = statement1.executeQuery(getTargetQuery);
      return compareResultSetandJsonData(rsTarget, bigQueryResponse);
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
   * Compares the data in the result set obtained from the Oracle database with the provided BigQuery JSON objects.
   *
   * @param rsSource     The result set obtained from the Oracle database.
   * @param bigQueryData The list of BigQuery JSON objects to compare with the result set data.
   * @return True if the result set data matches the BigQuery data, false otherwise.
   * @throws SQLException   If an SQL error occurs during the result set operations.
   * @throws ParseException If an error occurs while parsing the data.
   */
  public static boolean compareResultSetandJsonData(ResultSet rsSource, List<JsonObject> bigQueryData)
    throws SQLException, ParseException {
    ResultSetMetaData mdSource = rsSource.getMetaData();
    boolean result = false;
    int columnCountSource = mdSource.getColumnCount();

    if (bigQueryData == null) {
      Assert.fail("bigQueryData is null");
      return result;
    }
    // Get the column count of the first JsonObject in bigQueryData
    int jsonObjectIdx = 0;
    int columnCountTarget = 0;
    if (bigQueryData.size() > 0) {
      columnCountTarget = bigQueryData.get(jsonObjectIdx).entrySet().size();
    }
    // Compare the number of columns in the source and target
    Assert.assertEquals("Number of columns in source and target are not equal",
                        columnCountSource, columnCountTarget);

    while (rsSource.next()) {
      int currentColumnCount = 1;
      while (currentColumnCount <= columnCountSource) {
        String columnTypeName = mdSource.getColumnTypeName(currentColumnCount);
        int columnType = mdSource.getColumnType(currentColumnCount);
        String columnName = mdSource.getColumnName(currentColumnCount);
        // Perform different comparisons based on column type
        switch (columnType) {
          case Types.BIT:
            boolean bqDateString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBoolean();
            result = getBooleanValidation(rsSource, String.valueOf(bqDateString), columnName, columnTypeName);
            Assert.assertTrue("Different values found for column : %s", result);
            break;

          case Types.DECIMAL:
          case Types.NUMERIC:
            BigDecimal sourceDecimal = rsSource.getBigDecimal(currentColumnCount);
            BigDecimal targetDecimal = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBigDecimal();
            int desiredScale = 2; // Set the desired scale (number of decimal places)
            BigDecimal adjustedSourceValue = sourceDecimal.setScale(desiredScale, RoundingMode.HALF_UP);
            BigDecimal adjustedTargetValue = targetDecimal.setScale(desiredScale, RoundingMode.HALF_UP);
            Decimal sourceDecimalValue = Decimal.fromDecimal(adjustedSourceValue);
            Decimal targetDecimalValue = Decimal.fromDecimal(adjustedTargetValue);
            Assert.assertEquals("Different values found for column : %s", sourceDecimalValue, targetDecimalValue);
            break;

          case Types.REAL:
            float sourceReal = rsSource.getFloat(currentColumnCount);
            float targetReal = bigQueryData.get(jsonObjectIdx).get(columnName).getAsFloat();
            Assert.assertEquals(String.format("Different values found for column : %s", columnName), 0,
                                Float.compare(sourceReal, targetReal));
            break;

          case Types.TIMESTAMP:
            Timestamp sourceTS = rsSource.getTimestamp(columnName);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            Date parsedDate = dateFormat.parse(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Timestamp targetTs = new Timestamp(parsedDate.getTime());
            Assert.assertEquals(sourceTS, targetTs);
            break;

          case Types.TIME:
            String bqTimeString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            result = getTimeValidation(rsSource, bqTimeString, columnName, columnTypeName);
            Assert.assertTrue("Different values found for column : %s", result);
            break;

          case Types.BINARY:
          case Types.VARBINARY:
            String sourceB64String = new String(Base64.getEncoder().encode(rsSource.getBytes(currentColumnCount)));
            String targetB64String = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals("Different values found for column : %s",
                                sourceB64String, targetB64String);
            break;

          case Types.BIGINT:
            long sourceVal = rsSource.getLong(currentColumnCount);
            long targetVal = bigQueryData.get(jsonObjectIdx).get(columnName).getAsLong();
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceVal),
                                String.valueOf(targetVal));
            break;

          case Types.SMALLINT:
          case Types.TINYINT:
          case Types.INTEGER:
            int sourceInt = rsSource.getInt(currentColumnCount);
            int targetInt = bigQueryData.get(jsonObjectIdx).get(columnName).getAsInt();
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceInt),
                                String.valueOf(targetInt));
            break;

          case Types.DATE:
            Date dateSource = rsSource.getDate(currentColumnCount);
            Date dateTarget = java.sql.Date.valueOf(
              bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", dateSource, dateTarget);
            break;

          case Types.DOUBLE:
            Double sourceMoney = rsSource.getDouble(currentColumnCount);
            String targetMoneyStr = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Double targetMoney;
            // Remove non-numeric characters from the targetMoneyStr
            targetMoneyStr = targetMoneyStr.replaceAll("[^0-9.]", "");
            targetMoney = new Double(targetMoneyStr);
            Assert.assertEquals(String.format("Different values found for column: %s", columnName), 0,
                                sourceMoney.compareTo(targetMoney));
            break;

          default:
            String sourceValue = rsSource.getString(currentColumnCount);
            JsonElement jsonElement = bigQueryData.get(jsonObjectIdx).get(columnName);
            String targetValue = (jsonElement != null && !jsonElement.isJsonNull()) ? jsonElement.getAsString() : null;
            Assert.assertEquals(
              String.format("Different %s values found for column : %s", columnTypeName, columnName),
              String.valueOf(sourceValue), String.valueOf(targetValue));
        }
        currentColumnCount++;
      }
      jsonObjectIdx++;
    }
    Assert.assertFalse("Number of rows in Source table is greater than the number of rows in Target table",
                       rsSource.next());
    return true;
  }

  private static boolean getBooleanValidation(ResultSet rsSource, String bqDateString, String columnName,
                                              String columnTypeName) throws SQLException {
    switch (columnTypeName) {
      case "bit":
        byte source = rsSource.getByte(columnName);
        boolean sourceAsBoolean = source != 0;
        return String.valueOf(sourceAsBoolean).equals(String.valueOf(bqDateString));
      case "bool":
        boolean sourceValue = rsSource.getBoolean(columnName);
        return String.valueOf(sourceValue).equals(String.valueOf(bqDateString));
      default:
        return false;
    }
  }

  private static boolean getTimeValidation(ResultSet rsSource, String bqDateString, String columnName, String
    columnTypeName) throws SQLException {
    switch (columnTypeName) {
      case "time":
        Time sourceTime = rsSource.getTime(columnName);
        Time targetTime = Time.valueOf(bqDateString);
        return sourceTime.equals(targetTime);
      case "timetz":
        Time sourceT = rsSource.getTime(columnName);
        LocalTime sourceLocalTime = sourceT.toLocalTime();
        OffsetTime targetOffsetTime = OffsetTime.parse(bqDateString, DateTimeFormatter.ISO_OFFSET_TIME);
        LocalTime targetLocalTime = targetOffsetTime.toLocalTime();
        return String.valueOf(sourceLocalTime).equals(String.valueOf(targetLocalTime));

      default:
        return false;
    }
  }
}


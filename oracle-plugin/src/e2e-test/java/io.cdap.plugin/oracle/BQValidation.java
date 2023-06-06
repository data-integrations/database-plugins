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
package io.cdap.plugin.oracle;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.OracleClient;
import org.junit.Assert;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

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
    throws SQLException, ClassNotFoundException, ParseException, IOException, InterruptedException {
    List<JsonObject> jsonResponse = new ArrayList<>();
    List<Object> bigQueryRows = new ArrayList<>();
    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = new Gson().fromJson(String.valueOf(rows), JsonObject.class);
      jsonResponse.add(json);
    }
    String getSourceQuery = "SELECT * FROM " + schema + "." + sourceTable;
    try (Connection connect = OracleClient.getOracleConnection()) {
      connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);

      ResultSet rsSource = statement1.executeQuery(getSourceQuery);
      return compareResultSetAndJsonData(rsSource, jsonResponse);
    }
  }

  public static boolean validateBQToDBRecordValues(String schema, String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, ParseException, IOException, InterruptedException {
    List<JsonObject> jsonResponse = new ArrayList<>();
    List<Object> bigQueryRows = new ArrayList<>();
    getBigQueryTableData(sourceTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = new Gson().fromJson(String.valueOf(rows), JsonObject.class);
      jsonResponse.add(json);
    }
    String getTargetQuery = "SELECT * FROM " + schema + "." + targetTable;
    try (Connection connect = OracleClient.getOracleConnection()) {
      connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);

      ResultSet rsTarget = statement1.executeQuery(getTargetQuery);
      return compareResultSetAndJsonData(rsTarget, jsonResponse);
    }
  }

  /**
   * Retrieves the data from a specified BigQuery table and populates it into the provided list of objects.
   *
   * @param table The name of the BigQuery table to fetch data from.
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
      int currentColumnCount = 1;
      while (currentColumnCount <= columnCountSource) {
        String columnTypeName = mdSource.getColumnTypeName(currentColumnCount);
        int columnType = mdSource.getColumnType(currentColumnCount);
        String columnName = mdSource.getColumnName(currentColumnCount);
        // Perform different comparisons based on column type
        switch (columnType) {
          // Since we skip BFILE in Oracle Sink, we are not comparing the BFILE source and sink values
          case OracleSourceSchemaReader.BFILE:
            break;
          case Types.BLOB:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
            String sourceB64String = new String(Base64.getEncoder().encode(rsSource.getBytes(currentColumnCount)));
            String targetB64String = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals("Different values found for column : %s",
                                sourceB64String, targetB64String);
            break;

          case Types.NUMERIC:
            Number sourceVal = rsSource.getBigDecimal(currentColumnCount);
            Number targetVal = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBigDecimal();
            Assert.assertEquals(String.format("Different values found for column: %s", columnName),
                                sourceVal.intValue(), targetVal.intValue());
            break;

          case Types.TIMESTAMP:
            Timestamp sourceTS = rsSource.getTimestamp(columnName);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
            String targetT = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Date dateParsed = dateFormat.parse(targetT);
            Timestamp targetTs = new java.sql.Timestamp(dateParsed.getTime());
            result = String.valueOf(sourceTS).equals(String.valueOf(targetTs));
            Assert.assertTrue("Different values found for column : %s", result);
            break;

          case OracleSourceSchemaReader.TIMESTAMP_TZ:
            Timestamp sourceTZ = rsSource.getTimestamp(columnName);
            SimpleDateFormat dateValue = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            String targetTS = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Date date = dateValue.parse(targetTS);
            Timestamp targetTZ = new Timestamp(date.getTime());
            Assert.assertTrue("Different columns found for Timestamp", sourceTZ.equals(targetTZ));
            break;

          case OracleSourceSchemaReader.BINARY_FLOAT:
            Float sourceBytes = rsSource.getFloat(currentColumnCount);
            Float targetBytes = bigQueryData.get(jsonObjectIdx).get(columnName).getAsFloat();
            Assert.assertEquals(String.format("Different values found for column : %s", columnName), 0,
                                Float.compare(sourceBytes, targetBytes));
            break;

          case OracleSourceSchemaReader.BINARY_DOUBLE:
            double sourceVAL = rsSource.getDouble(currentColumnCount);
            double targetVAL = bigQueryData.get(jsonObjectIdx).get(columnName).getAsDouble();
            Assert.assertEquals(String.format("Different values found for column : %s", columnName), 0,
                                Double.compare(sourceVAL, targetVAL));
            break;

          default:
            String sourceString = rsSource.getString(currentColumnCount);
            String targetString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals(String.format("Different %s values found for column : %s", columnTypeName, columnName),
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

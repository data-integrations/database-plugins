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

//package io.cdap.plugin.oracle;
//
//import com.simba.googlebigquery.jdbc.DataSource;
//import io.cdap.e2e.utils.PluginPropertyUtils;
//import io.cdap.plugin.OracleClient;
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//
///**
// *  Big Query client.
// */
//public class BQValidation {
//
//  /**
//   * Extracts entire data from source and target tables.
//   * @param sourceTable table at the source side
//   * @param targetTable table at the BigQuery side
//   * @return true if the values in source and target side are equal
//   */
//  public static boolean validateBQAndDBRecordValues(String schema, String sourceTable, String targetTable)
//    throws SQLException, ClassNotFoundException {
//    String getSourceQuery = "SELECT * FROM " + schema + "." + sourceTable;
//
//    try (Connection connect = OracleClient.getOracleConnection()) {
//      connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
//      Statement statement1 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
//                                                     ResultSet.HOLD_CURSORS_OVER_COMMIT);
//
//      ResultSet rsSource = statement1.executeQuery(getSourceQuery);
//      ResultSet rsTarget = getBigQueryDataAsResultSet(targetTable);
//
//      return OracleClient.compareResultSetData(rsSource, rsTarget);
//    }
//  }
//
//  public static ResultSet getBigQueryDataAsResultSet(String targetTable) throws SQLException {
//    Connection connection = null;
//    DataSource dataSource = new com.simba.googlebigquery.jdbc.DataSource();
//    String projectId = PluginPropertyUtils.pluginProp("projectId");
//    String datasetId = PluginPropertyUtils.pluginProp("dataset");
//
//    String jdbcUrl = String.format(PluginPropertyUtils.pluginProp("jdbcUrl"), projectId);
//    dataSource.setURL(jdbcUrl);
//    connection = dataSource.getConnection();
//    Statement statement = connection.createStatement();
//    ResultSet bqResultSet = statement.executeQuery("SELECT * from " + datasetId +  "." + targetTable +  ";");
//
//    return bqResultSet;
//  }
//}

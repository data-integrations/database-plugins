/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleSourceTestRun extends OraclePluginTestBase {

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBMacroSupport() throws Exception {
    String importQuery = "SELECT * FROM my_table WHERE DATE_COL <= " +
      "TO_DATE('${logicalStartTime(yyyy-MM-dd,1d)}', 'yyyy-MM-dd') " +
      "AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(SMALLINT_COL),MAX(SMALLINT_COL) from my_table";
    String splitBy = "SMALLINT_COL";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .putAll(BASE_PROPS)
      .put(OracleConstants.DEFAULT_ROW_PREFETCH, "40")
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
      .put(Constants.Reference.REFERENCE_NAME, "DBTestSource").build();

    ETLPlugin sourceConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("macroOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBMacro");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("macroOutputTable");
    Assert.assertTrue(MockSink.readOutput(outputManager).isEmpty());
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBSource() throws Exception {
    String importQuery = "SELECT CHAR_COL, VARCHAR_COL, VARCHAR2_COL, NVARCHAR2_COL, INT_COL, INTEGER_COL, DEC_COL, " +
      "DECIMAL_COL, NUMBER_COL, NUMERIC_COL, SMALLINT_COL, REAL_COL, DATE_COL, TIMESTAMP_COL, " +
      "INTERVAL_YEAR_TO_MONTH_COL, INTERVAL_DAY_TO_SECOND_COL, RAW_COL, TIMESTAMPTZ_COL, TIMESTAMPLTZ_COL, CLOB_COL, " +
      "NCLOB_COL, BLOB_COL, NCHAR_COL, FLOAT_COL, ROWID, ROWID_COL, UROWID_COL, BINARY_FLOAT_COL, BINARY_DOUBLE_COL, " +
      "LONG_RAW_COL, BFILE_COL FROM my_table WHERE SMALLINT_COL < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(SMALLINT_COL),MAX(SMALLINT_COL) from my_table";
    String splitBy = "SMALLINT_COL";
    ETLPlugin sourceConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(OracleConstants.DEFAULT_ROW_PREFETCH, "40")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBSourceTest")
        .build(),
      null
    );

    String outputDatasetName = "output-dbsourcetest";
    ETLPlugin sinkConfig = MockSink.getPlugin(outputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBSource");
    runETLOnce(appManager);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("VARCHAR_COL");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Built-in column
    Assert.assertNotNull(row1.get("ROWID"));
    Assert.assertNotNull(row2.get("ROWID"));

    Assert.assertEquals("AAAUEVAAFAAAAR/AA" + 1, row1.get("ROWID_COL").toString());
    Assert.assertEquals("AAAUEVAAFAAAAR/AA" + 2, row2.get("ROWID_COL").toString());
    Assert.assertEquals("AAAUEVAAFAAAAR/AA" + 1, row1.get("UROWID_COL").toString());
    Assert.assertEquals("AAAUEVAAFAAAAR/AA" + 2, row2.get("UROWID_COL").toString());

    // Verify data
    Assert.assertEquals("user1", row1.get("CHAR_COL").toString().trim());
    Assert.assertEquals("user2", row2.get("CHAR_COL").toString().trim());
    Assert.assertEquals("user1", row1.get("NCHAR_COL").toString().trim());
    Assert.assertEquals("user2", row2.get("NCHAR_COL").toString().trim());
    Assert.assertEquals("user1", row1.get("VARCHAR_COL"));
    Assert.assertEquals("user2", row2.get("VARCHAR_COL"));
    Assert.assertEquals("user1", row1.get("VARCHAR2_COL"));
    Assert.assertEquals("user2", row2.get("VARCHAR2_COL"));
    Assert.assertEquals("user1", row1.get("NVARCHAR2_COL"));
    Assert.assertEquals("user2", row2.get("NVARCHAR2_COL"));

    Assert.assertEquals(new BigDecimal(43, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row1.getDecimal("INT_COL"));
    Assert.assertEquals(new BigDecimal(44, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row2.getDecimal("INT_COL"));
    Assert.assertEquals(new BigDecimal(25, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row1.getDecimal("INTEGER_COL"));
    Assert.assertEquals(new BigDecimal(26, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row2.getDecimal("INTEGER_COL"));
    Assert.assertEquals(new BigDecimal(56, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row1.getDecimal("DEC_COL"));
    Assert.assertEquals(new BigDecimal(57, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row2.getDecimal("DEC_COL"));
    Assert.assertEquals(new BigDecimal(1, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row1.getDecimal("SMALLINT_COL"));
    Assert.assertEquals(new BigDecimal(2, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row2.getDecimal("SMALLINT_COL"));

    Assert.assertEquals(124.45, row1.get("FLOAT_COL"), 0.00001);
    Assert.assertEquals(125.45, row2.get("FLOAT_COL"), 0.00001);
    Assert.assertEquals(new BigDecimal(55.65, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("DECIMAL_COL"));
    Assert.assertEquals(new BigDecimal(56.65, new MathContext(PRECISION)).setScale(SCALE),
                        row2.getDecimal("DECIMAL_COL"));
    Assert.assertEquals(new BigDecimal(33.65, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("NUMBER_COL"));
    Assert.assertEquals(new BigDecimal(34.65, new MathContext(PRECISION)).setScale(SCALE),
                        row2.getDecimal("NUMBER_COL"));
    Assert.assertEquals(new BigDecimal(24.65, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("NUMERIC_COL"));
    Assert.assertEquals(new BigDecimal(25.65, new MathContext(PRECISION)).setScale(SCALE),
                        row2.getDecimal("NUMERIC_COL"));

    Assert.assertEquals(15.45, row1.get("REAL_COL"), 0.000001);
    Assert.assertEquals(16.45, row2.get("REAL_COL"), 0.000001);

    // Verify time columns
    java.util.Date date = new java.util.Date(CURRENT_TS);

    ZonedDateTime expectedTs = date.toInstant().atZone(UTC);
    Assert.assertEquals(expectedTs.withNano(0), row1.getTimestamp("DATE_COL"));
    Assert.assertEquals(expectedTs, row1.getTimestamp("TIMESTAMP_COL", UTC));
    Assert.assertEquals("2019-07-15 15:57:46.65 GMT", row1.get("TIMESTAMPTZ_COL"));
    Assert.assertEquals(expectedTs, row1.getTimestamp("TIMESTAMPLTZ_COL", UTC));

    // Oracle specific types
    Assert.assertEquals("300-5", row1.get("INTERVAL_YEAR_TO_MONTH_COL").toString().trim());
    Assert.assertEquals("23 3:2:10.0", row1.get("INTERVAL_DAY_TO_SECOND_COL").toString().trim());

    // verify binary columns
    Assert.assertEquals("user1", Bytes.toString((ByteBuffer) row1.get("RAW_COL")));
    Assert.assertEquals("user2", Bytes.toString((ByteBuffer) row2.get("RAW_COL")));
    Assert.assertEquals("user1", Bytes.toString((ByteBuffer) row1.get("LONG_RAW_COL")));
    Assert.assertEquals("user2", Bytes.toString((ByteBuffer) row2.get("LONG_RAW_COL")));
    Assert.assertEquals("user1", row1.get("CLOB_COL"));
    Assert.assertEquals("user2", row2.get("CLOB_COL"));
    Assert.assertEquals("user1", row1.get("NCLOB_COL"));
    Assert.assertEquals("user2", row2.get("NCLOB_COL"));
    Assert.assertEquals("user1", Bytes.toString((ByteBuffer) row1.get("BLOB_COL")));
    Assert.assertEquals("user2", Bytes.toString((ByteBuffer) row2.get("BLOB_COL")));
    Assert.assertEquals(124.45f, (float) row1.get("BINARY_FLOAT_COL"), 0.000001);
    Assert.assertEquals(125.45f, (float) row2.get("BINARY_FLOAT_COL"), 0.000001);
    Assert.assertEquals(124.45, row1.get("BINARY_DOUBLE_COL"), 0.000001);
    Assert.assertEquals(125.45, row2.get("BINARY_DOUBLE_COL"), 0.000001);
    Assert.assertNull(row2.get("BFILE_COL"));
  }

  @Test
  public void testDbSourceLongColumn() throws Exception {
    String importQuery = "SELECT VARCHAR_COL, LONG_COL FROM " + MY_TABLE_FOR_LONG +
      " WHERE SMALLINT_COL < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(SMALLINT_COL),MAX(SMALLINT_COL) from " + MY_TABLE_FOR_LONG;
    String splitBy = "SMALLINT_COL";
    ETLPlugin sourceConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(OracleConstants.DEFAULT_ROW_PREFETCH, "40")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBSourceLongTest")
        .build(),
      null
    );

    String outputDatasetName = "output-dbsourcetest-long";
    ETLPlugin sinkConfig = MockSink.getPlugin(outputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBSourceLong");
    runETLOnce(appManager);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("VARCHAR_COL");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    Assert.assertEquals("user1", row1.get("LONG_COL").toString().trim());
    Assert.assertEquals("user2", row2.get("LONG_COL").toString().trim());
  }

  @Test
  public void testDbSourceMultipleTables() throws Exception {
    String importQuery = "SELECT my_table.ID, your_table.VARCHAR_COL FROM my_table, your_table " +
      "WHERE my_table.ID < 3 and my_table.ID = your_table.ID and $CONDITIONS ";
    String boundingQuery = "SELECT LEAST(MIN(my_table.ID), MIN(your_table.ID)), " +
      "GREATEST(MAX(my_table.ID), MAX(your_table.ID))";
    String splitBy = "my_table.ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(OracleConstants.DEFAULT_ROW_PREFETCH, "40")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBMultipleTest")
        .build(),
      null
    );

    String outputDatasetName = "output-multitabletest";
    ETLPlugin sinkConfig = MockSink.getPlugin(outputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBSourceWithMultipleTables");
    runETLOnce(appManager);

    // records should be written
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("VARCHAR_COL");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    Assert.assertEquals("user1", row1.get("VARCHAR_COL"));
    Assert.assertEquals("user2", row2.get("VARCHAR_COL"));
    Assert.assertEquals(new BigDecimal(1, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row1.getDecimal("ID"));
    Assert.assertEquals(new BigDecimal(2, new MathContext(DEFAULT_PRECISION)).setScale(0),
                        row2.getDecimal("ID"));
  }

  @Test
  public void testUserNamePasswordCombinations() throws Exception {
    String importQuery = "SELECT * FROM my_table WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(SMALLINT_COL),MAX(SMALLINT_COL) from my_table";
    String splitBy = "SMALLINT_COL";

    ETLPlugin sinkConfig = MockSink.getPlugin("outputTable");

    Map<String, String> baseSourceProps = ImmutableMap.<String, String>builder()
      .put(OracleConstants.DEFAULT_ROW_PREFETCH, "40")
      .put(OracleConstants.DEFAULT_BATCH_VALUE, "40")
      .put(OracleConstants.CONNECTION_TYPE, BASE_PROPS.get(OracleConstants.CONNECTION_TYPE))
      .put(ConnectionConfig.HOST, BASE_PROPS.get(ConnectionConfig.HOST))
      .put(ConnectionConfig.PORT, BASE_PROPS.get(ConnectionConfig.PORT))
      .put(ConnectionConfig.DATABASE, BASE_PROPS.get(ConnectionConfig.DATABASE))
      .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
      .put(Constants.Reference.REFERENCE_NAME, "UserPassDBTest")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("dbTest");

    // null user name, null password. Should succeed.
    // as source
    ETLPlugin dbConfig = new ETLPlugin(OracleConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, baseSourceProps, null);
    ETLStage table = new ETLStage("uniqueTableSink", sinkConfig);
    ETLStage database = new ETLStage("databaseSource", dbConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(database)
      .addStage(table)
      .addConnection(database.getName(), table.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    deployApplication(appId, appRequest);

    // null user name, non-null password. Should fail.
    // as source
    Map<String, String> noUser = new HashMap<>(baseSourceProps);
    noUser.put(ConnectionConfig.PASSWORD, "password");
    database = new ETLStage("databaseSource", new ETLPlugin(OracleConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
                                                            noUser, null));
    etlConfig = ETLBatchConfig.builder()
      .addStage(database)
      .addStage(table)
      .addConnection(database.getName(), table.getName())
      .build();
    assertDeploymentFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT,
                            "Deploying DB Source with null username but non-null password should have failed.");

    // non-null username, non-null, but empty password. Should succeed.
    // as source
    Map<String, String> emptyPassword = new HashMap<>(baseSourceProps);
    emptyPassword.put(ConnectionConfig.USER, "root");
    emptyPassword.put(ConnectionConfig.PASSWORD, "");
    database = new ETLStage("databaseSource", new ETLPlugin(OracleConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
                                                            emptyPassword, null));
    etlConfig = ETLBatchConfig.builder()
      .addStage(database)
      .addStage(table)
      .addConnection(database.getName(), table.getName())
      .build();
    appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    deployApplication(appId, appRequest);
  }

  @Test
  public void testNonExistentDBTable() throws Exception {
    // source
    String importQuery = "SELECT ID, NAME FROM dummy WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) FROM dummy";
    String splitBy = "ID";
    ETLPlugin sinkConfig = MockSink.getPlugin("table");
    ETLPlugin sourceBadNameConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(OracleConstants.DEFAULT_ROW_PREFETCH, "40")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBNonExistentTest")
        .build(),
      null);
    ETLStage sink = new ETLStage("sink", sinkConfig);
    ETLStage sourceBadName = new ETLStage("sourceBadName", sourceBadNameConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(sourceBadName)
      .addStage(sink)
      .addConnection(sourceBadName.getName(), sink.getName())
      .build();
    ApplicationId appId = NamespaceId.DEFAULT.app("dbSourceNonExistingTest");
    assertRuntimeFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT,
                         "ETL Application with DB Source should have failed because of a " +
                           "non-existent source table.", 1);

    // Bad connection
    ETLPlugin sourceBadConnConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(OracleConstants.DEFAULT_ROW_PREFETCH, "40")
        .put(OracleConstants.DEFAULT_BATCH_VALUE, "40")
        .put(OracleConstants.CONNECTION_TYPE, BASE_PROPS.get(OracleConstants.CONNECTION_TYPE))
        .put(ConnectionConfig.HOST, BASE_PROPS.get(ConnectionConfig.HOST))
        .put(ConnectionConfig.PORT, BASE_PROPS.get(ConnectionConfig.PORT))
        .put(ConnectionConfig.DATABASE, "dumDB")
        .put(ConnectionConfig.USER, BASE_PROPS.get(ConnectionConfig.USER))
        .put(ConnectionConfig.PASSWORD, BASE_PROPS.get(ConnectionConfig.PASSWORD))
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "MySQLTest")
        .build(),
      null);
    ETLStage sourceBadConn = new ETLStage("sourceBadConn", sourceBadConnConfig);
    etlConfig = ETLBatchConfig.builder()
      .addStage(sourceBadConn)
      .addStage(sink)
      .addConnection(sourceBadConn.getName(), sink.getName())
      .build();
    assertRuntimeFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT,
                         "ETL Application with DB Source should have failed because of a " +
                           "non-existent source database.", 2);
  }
}

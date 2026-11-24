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

package io.cdap.plugin.db.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.GenericDatabasePluginTestBase;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.source.AbstractDBSource;
import io.cdap.plugin.jdbc.DatabaseConstants;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for ETL using databases.
 */
public class DBSourceTestRun extends GenericDatabasePluginTestBase {

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBMacroSupport() throws Exception {
    String importQuery = "SELECT * FROM \"my_table\" WHERE DATE_COL <= '${logicalStartTime(yyyy-MM-dd,1d)}' " +
      "AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from \"my_table\"";
    String splitBy = "ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBMacroTest")
        .build(),
      null
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("macroOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, "testDBMacro");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("macroOutputTable");
    Assert.assertTrue(MockSink.readOutput(outputManager).isEmpty());
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBSource() throws Exception {
    String importQuery = "SELECT ID, NAME, SCORE, GRADUATED, TINY, SMALL, BIG, FLOAT_COL, REAL_COL, NUMERIC_COL, " +
      "DECIMAL_COL, BIT_COL, DATE_COL, TIME_COL, TIMESTAMP_COL, BINARY_COL, LONGVARBINARY_COL, BLOB_COL, " +
      "CLOB_COL, CHAR_COL, LONGVARCHAR_COL, VARBINARY_COL FROM \"my_table\"" +
      "WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from \"my_table\"";
    String splitBy = "ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBSourceTest")
        .build(),
      null
    );

    String outputDatasetName = "output-dbsourcetest";
    ETLPlugin sinkConfig = MockSink.getPlugin(outputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, "testDBSource");
    runETLOnce(appManager);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("NAME");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    Assert.assertEquals("user1", row1.get("NAME"));
    Assert.assertEquals("user2", row2.get("NAME"));
    Assert.assertEquals("longvarchar1", row1.get("LONGVARCHAR_COL"));
    Assert.assertEquals("longvarchar2", row2.get("LONGVARCHAR_COL"));
    Assert.assertEquals("char1", ((String) row1.get("CHAR_COL")).trim());
    Assert.assertEquals("char2", ((String) row2.get("CHAR_COL")).trim());
    Assert.assertEquals(124.45, (double) row1.get("SCORE"), 0.000001);
    Assert.assertEquals(125.45, (double) row2.get("SCORE"), 0.000001);
    Assert.assertEquals(false, row1.get("GRADUATED"));
    Assert.assertEquals(true, row2.get("GRADUATED"));
    Assert.assertNull(row1.get("NOT_IMPORTED"));
    Assert.assertNull(row2.get("NOT_IMPORTED"));
    // TODO: Reading from table as SHORT seems to be giving the wrong value.
    Assert.assertEquals(1, (int) row1.get("TINY"));
    Assert.assertEquals(2, (int) row2.get("TINY"));
    Assert.assertEquals(1, (int) row1.get("SMALL"));
    Assert.assertEquals(2, (int) row2.get("SMALL"));
    Assert.assertEquals(1, (long) row1.get("BIG"));
    Assert.assertEquals(2, (long) row2.get("BIG"));
    // TODO: Reading from table as FLOAT seems to be giving back the wrong value.
    Assert.assertEquals(124.45, (double) row1.get("FLOAT_COL"), 0.00001);
    Assert.assertEquals(125.45, (double) row2.get("FLOAT_COL"), 0.00001);
    Assert.assertEquals(124.45, (double) row1.get("REAL_COL"), 0.00001);
    Assert.assertEquals(125.45, (double) row2.get("REAL_COL"), 0.00001);
    Assert.assertEquals(new BigDecimal(124.45, new MathContext(PRECISION)).setScale(SCALE),
      row1.getDecimal("NUMERIC_COL"));
    Assert.assertEquals(new BigDecimal(125.45, new MathContext(PRECISION)).setScale(SCALE),
                        row2.getDecimal("NUMERIC_COL"));
    Assert.assertEquals(new BigDecimal(124.45, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("DECIMAL_COL"));
    Assert.assertNull(row2.getDecimal("DECIMAL_COL"));
    Assert.assertTrue((boolean) row1.get("BIT_COL"));
    Assert.assertFalse((boolean) row2.get("BIT_COL"));
    // Verify time columns
    java.util.Date date = new java.util.Date(CURRENT_TS);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    LocalDate expectedDate = Date.valueOf(sdf.format(date)).toLocalDate();
    sdf = new SimpleDateFormat("H:mm:ss");
    LocalTime expectedTime = Time.valueOf(sdf.format(date)).toLocalTime();
    ZonedDateTime expectedTs = date.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    Assert.assertEquals(expectedDate, row1.getDate("DATE_COL"));
    Assert.assertEquals(expectedTime, row1.getTime("TIME_COL"));
    Assert.assertEquals(expectedTs, row1.getTimestamp("TIMESTAMP_COL", ZoneId.ofOffset("UTC", ZoneOffset.UTC)));

    // verify binary columns
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("BINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("BINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("LONGVARBINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("LONGVARBINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("VARBINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("VARBINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("BLOB_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("BLOB_COL")).array(), 0, 5));
    Assert.assertEquals(CLOB_DATA, row1.get("CLOB_COL"));
    Assert.assertEquals(CLOB_DATA, row2.get("CLOB_COL"));
  }

  @Test
  public void testDBSourceWithLowerCaseColNames() throws Exception {
    String importQuery = "SELECT ID, NAME FROM \"my_table\" WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from \"my_table\"";
    String splitBy = "ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBLowerCaseTest")
        .build(),
      null
    );

    String outputDatasetName = "output-lowercasetest";
    ETLPlugin sinkConfig = MockSink.getPlugin(outputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBSourceWithLowerCase");
    runETLOnce(appManager);

    // records should be written
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("NAME");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);
    // Verify data
    Assert.assertEquals("user1", row1.get("NAME"));
    Assert.assertEquals("user2", row2.get("NAME"));
    Assert.assertEquals(1, row1.<Integer>get("ID").intValue());
    Assert.assertEquals(2, row2.<Integer>get("ID").intValue());
  }

  @Test
  public void testDbSourceMultipleTables() throws Exception {
    // have the same data in both tables ('\"my_table\"' and '\"your_table\"'), and select the ID and NAME fields from
    // separate tables
    String importQuery = "SELECT \"my_table\".ID, \"your_table\".NAME FROM \"my_table\", \"your_table\"" +
      "WHERE \"my_table\".ID < 3 and \"my_table\".ID = \"your_table\".ID and $CONDITIONS";
    String boundingQuery = "SELECT MIN(MIN(\"my_table\".ID), MIN(\"your_table\".ID)), " +
      "MAX(MAX(\"my_table\".ID), MAX(\"your_table\".ID))";
    String splitBy = "\"my_table\".ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      "Database",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
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
    String userid = outputRecords.get(0).get("NAME");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);
    // Verify data
    Assert.assertEquals("user1", row1.get("NAME"));
    Assert.assertEquals("user2", row2.get("NAME"));
    Assert.assertEquals(1, row1.<Integer>get("ID").intValue());
    Assert.assertEquals(2, row2.<Integer>get("ID").intValue());
  }

  @Test
  public void testUserNamePasswordCombinations() throws Exception {
    String importQuery = "SELECT * FROM \"my_table\" WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from \"my_table\"";
    String splitBy = "ID";

    ETLPlugin sinkConfig = MockSink.getPlugin("outputTable");

    Map<String, String> baseSourceProps = ImmutableMap.<String, String>builder()
      .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
      .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
      .put(Constants.Reference.REFERENCE_NAME, "UserPassDBTest")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("dbTest");

    // null user name, null password. Should succeed.
    // as source
    ETLPlugin dbConfig = new ETLPlugin(DatabaseConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, baseSourceProps, null);
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
    database = new ETLStage("databaseSource",
                            new ETLPlugin(DatabaseConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, noUser, null));
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
    emptyPassword.put(ConnectionConfig.USER, "emptyPwdUser");
    emptyPassword.put(ConnectionConfig.PASSWORD, "");
    database = new ETLStage("databaseSource",
                            new ETLPlugin(DatabaseConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, emptyPassword, null));
    etlConfig = ETLBatchConfig.builder()
      .addStage(database)
      .addStage(table)
      .addConnection(database.getName(), table.getName())
      .build();
    appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    deployApplication(appId, appRequest);
  }

  @Test
  public void testNonSelectImportQueryFailsDeployment() throws Exception {
    int rowCount = countRows("my_table");
    String importQuery = "SELECT * FROM \"my_table\" WHERE $CONDITIONS; UPDATE \"my_table\" SET NAME='INJECTED'";
    ETLPlugin sourceConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, "SELECT MIN(ID),MAX(ID) from \"my_table\"")
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, "ID")
        .put(Constants.Reference.REFERENCE_NAME, "DBNonSelectQueryTest")
        .build(),
      null);
    ETLBatchConfig etlConfig = getETLBatchConfig(sourceConfig, MockSink.getPlugin("nonSelectQueryOutput"));
    assertDeploymentFailureContains(NamespaceId.DEFAULT.app("dbSourceNonSelectQueryTest"), etlConfig,
                                    "Invalid Import Query");
    Assert.assertEquals(rowCount, countRows("my_table"));
    Assert.assertEquals(0, countRowsWithName("my_table", "INJECTED"));
  }

  @Test
  public void testNonSelectImportQueryWithExplicitSchemaFailsDeployment() throws Exception {
    // When an explicit schema is configured, configurePipeline returns before opening any DB connection, so only
    // SelectQueryValidator can fail deployment here.
    Schema explicitSchema = Schema.recordOf(
      "explicit",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)));
    String importQuery = "SELECT ID, NAME FROM \"my_table\" WHERE $CONDITIONS; UPDATE \"my_table\" SET NAME='INJECTED'";
    ETLPlugin sourceConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "1")
        .put(AbstractDBSource.DBSourceConfig.SCHEMA, explicitSchema.toString())
        .put(Constants.Reference.REFERENCE_NAME, "DBExplicitSchemaNonSelectTest")
        .build(),
      null);
    ETLBatchConfig etlConfig = getETLBatchConfig(sourceConfig, MockSink.getPlugin("explicitSchemaNonSelectOutput"));
    assertDeploymentFailureContains(NamespaceId.DEFAULT.app("dbSourceExplicitSchemaNonSelectTest"), etlConfig,
                                    "Invalid Import Query");
    Assert.assertEquals(0, countRowsWithName("my_table", "INJECTED"));
  }

  @Test
  public void testNonSelectImportQueryMacroFailsRun() throws Exception {
    int rowCount = countRows("my_table");
    ETLPlugin sourceConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, "${importQuery}")
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, "SELECT MIN(ID),MAX(ID) from \"my_table\"")
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, "ID")
        .put(Constants.Reference.REFERENCE_NAME, "DBNonSelectQueryMacroTest")
        .build(),
      null);
    ApplicationManager appManager = deployETL(sourceConfig, MockSink.getPlugin("nonSelectQueryMacroOutput"),
                                              DATAPIPELINE_ARTIFACT, "dbSourceNonSelectQueryMacroTest");
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of(
      "importQuery",
      "SELECT * FROM \"my_table\" WHERE $CONDITIONS; DELETE FROM \"my_table\"; UPDATE \"my_table\" SET NAME='INJECTED'"
    ));
    workflowManager.waitForRun(ProgramRunStatus.FAILED, 5, TimeUnit.MINUTES);
    Assert.assertEquals(rowCount, countRows("my_table"));
    Assert.assertEquals(0, countRowsWithName("my_table", "INJECTED"));
  }

  @Test
  public void testNonSelectBoundingQueryFailsDeploymentAndMacroFailsRun() throws Exception {
    int rowCount = countRows("my_table");
    ETLPlugin nonMacroBoundingConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, "SELECT * FROM \"my_table\" WHERE $CONDITIONS")
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY,
             "SELECT MIN(ID), MAX(ID) FROM \"my_table\"; DELETE FROM \"my_table\"")
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, "ID")
        .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "2")
        .put(Constants.Reference.REFERENCE_NAME, "DBNonSelectBoundingQueryDeployTest")
        .build(),
      null);
    ETLBatchConfig deployConfig = getETLBatchConfig(nonMacroBoundingConfig,
                                                    MockSink.getPlugin("nonSelectBoundingDeployOutput"));
    assertDeploymentFailureContains(NamespaceId.DEFAULT.app("dbSourceNonSelectBoundingDeployTest"), deployConfig,
                                    "Invalid Bounding Query");
    Assert.assertEquals(rowCount, countRows("my_table"));

    ETLPlugin macroBoundingConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, "SELECT * FROM \"my_table\" WHERE $CONDITIONS")
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, "${boundingQuery}")
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, "ID")
        .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "2")
        .put(Constants.Reference.REFERENCE_NAME, "DBNonSelectBoundingQueryMacroTest")
        .build(),
      null);
    ApplicationManager appManager = deployETL(macroBoundingConfig, MockSink.getPlugin("nonSelectBoundingMacroOutput"),
                                              DATAPIPELINE_ARTIFACT, "dbSourceNonSelectBoundingMacroTest");
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of(
      "boundingQuery",
      "SELECT MIN(ID), MAX(ID) FROM \"my_table\"; DELETE FROM \"my_table\"; UPDATE \"my_table\" SET NAME='INJECTED'"));
    workflowManager.waitForRun(ProgramRunStatus.FAILED, 5, TimeUnit.MINUTES);
    Assert.assertEquals(rowCount, countRows("my_table"));
    Assert.assertEquals(0, countRowsWithName("my_table", "INJECTED"));
  }

  @Test
  public void testInjectionSplitByFailsDeployment() throws Exception {
    int rowCount = countRows("my_table");
    String splitBy = "ID); UPDATE \"my_table\" SET NAME='INJECTED' WHERE (1=1";
    ETLPlugin sourceConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, "SELECT * FROM \"my_table\" WHERE $CONDITIONS")
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, "SELECT MIN(ID),MAX(ID) FROM \"my_table\"")
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "2")
        .put(Constants.Reference.REFERENCE_NAME, "DBInjectionSplitByTest")
        .build(),
      null);
    ETLBatchConfig etlConfig = getETLBatchConfig(sourceConfig, MockSink.getPlugin("injectionSplitByOutput"));
    assertDeploymentFailureContains(NamespaceId.DEFAULT.app("dbSourceInjectionSplitByTest"), etlConfig,
                                    "Invalid Split-By Field Name");
    Assert.assertEquals(rowCount, countRows("my_table"));
    Assert.assertEquals(0, countRowsWithName("my_table", "INJECTED"));
  }

  private static volatile String sideEffectJdbcUrl;

  public static int writeSideEffect(Connection sessionConn) throws SQLException {
    if (sessionConn.isReadOnly()) {
      throw new SQLException("SQL data change not permitted in read-only session", "25006");
    }
    String url = sideEffectJdbcUrl;
    if (url != null) {
      try (Connection rwConn = DriverManager.getConnection(url, "SA", "");
           Statement stmt = rwConn.createStatement()) {
        stmt.executeUpdate("UPDATE \"my_table\" SET NAME = 'INJECTED' WHERE ID = 1");
      }
    }
    return 1;
  }

  @Test
  public void testReadOnlyConnectionBlocksWritingFunctionInSelect() throws Exception {
    sideEffectJdbcUrl = getConnectionURL();
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE FUNCTION WRITE_SIDE_EFFECT() RETURNS INT " +
                     "LANGUAGE JAVA NOT DETERMINISTIC READS SQL DATA " +
                     "EXTERNAL NAME 'CLASSPATH:io.cdap.plugin.db.sink.DBSourceTestRun.writeSideEffect'");
    }
    try {
      String queryWithWritingFn = "SELECT WRITE_SIDE_EFFECT() AS SIDE_EFFECT, ID FROM \"my_table\" WHERE $CONDITIONS";

      // 1. Configure-time schema fetch opens a read-only connection, blocking the writing function.
      ETLPlugin deployTimeSource = new ETLPlugin(
        DatabaseConstants.PLUGIN_NAME,
        BatchSource.PLUGIN_TYPE,
        ImmutableMap.<String, String>builder()
          .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
          .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
          .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, queryWithWritingFn)
          .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "1")
          .put(Constants.Reference.REFERENCE_NAME, "DBReadOnlyDeployBackstopTest")
          .build(),
        null);
      ETLBatchConfig deployConfig = getETLBatchConfig(deployTimeSource, MockSink.getPlugin("readOnlyDeployOutput"));
      assertDeploymentFailure(NamespaceId.DEFAULT.app("dbSourceReadOnlyDeployTest"), deployConfig,
                              DATAPIPELINE_ARTIFACT,
                              "Read-only connection should have blocked writing function during schema fetch.");
      Assert.assertEquals(0, countRowsWithName("my_table", "INJECTED"));

      // 2. Even with an explicit schema (skipping configure-time fetch), runtime execution is also read-only.
      Schema explicitSchema = Schema.recordOf(
        "explicitFn",
        Schema.Field.of("SIDE_EFFECT", Schema.of(Schema.Type.INT)),
        Schema.Field.of("ID", Schema.of(Schema.Type.INT)));
      ETLPlugin runTimeSource = new ETLPlugin(
        DatabaseConstants.PLUGIN_NAME,
        BatchSource.PLUGIN_TYPE,
        ImmutableMap.<String, String>builder()
          .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
          .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
          .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, queryWithWritingFn)
          .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "1")
          .put(AbstractDBSource.DBSourceConfig.SCHEMA, explicitSchema.toString())
          .put(Constants.Reference.REFERENCE_NAME, "DBReadOnlyRunBackstopTest")
          .build(),
        null);
      ApplicationManager appManager = deployETL(runTimeSource, MockSink.getPlugin("readOnlyRunOutput"),
                                                DATAPIPELINE_ARTIFACT, "dbSourceReadOnlyRunTest");
      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start();
      workflowManager.waitForRun(ProgramRunStatus.FAILED, 5, TimeUnit.MINUTES);
      Assert.assertEquals(0, countRowsWithName("my_table", "INJECTED"));
    } finally {
      sideEffectJdbcUrl = null;
      try (Connection conn = getConnection();
           Statement stmt = conn.createStatement()) {
        stmt.execute("DROP FUNCTION IF EXISTS WRITE_SIDE_EFFECT");
      }
    }
  }

  @Test
  public void testDisableQueryValidationAllowsTrustedLegacyQuery() throws Exception {
    // HSQLDB treats '/*M! ... */' as a regular comment, whereas SelectQueryValidator rejects it by default.
    String legacyQuery = "SELECT ID, NAME FROM \"my_table\" /*M! legacy hint */ WHERE ID = 1 AND $CONDITIONS";

    ETLPlugin defaultSource = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, legacyQuery)
        .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "1")
        .put(Constants.Reference.REFERENCE_NAME, "DBLegacyRejectedByDefaultTest")
        .build(),
      null);
    ETLBatchConfig rejectedConfig = getETLBatchConfig(defaultSource, MockSink.getPlugin("legacyRejectedOutput"));
    assertDeploymentFailureContains(NamespaceId.DEFAULT.app("dbSourceLegacyRejectedTest"), rejectedConfig,
                                    "Invalid Import Query");

    ETLPlugin optOutSource = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(ConnectionConfig.DISABLE_QUERY_VALIDATION, "true")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, legacyQuery)
        .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "1")
        .put(Constants.Reference.REFERENCE_NAME, "DBLegacyOptOutAllowedTest")
        .build(),
      null);
    ApplicationManager appManager = deployETL(optOutSource, MockSink.getPlugin("legacyOptOutOutput"),
                                              DATAPIPELINE_ARTIFACT, "dbSourceLegacyOptOutTest");
    runETLOnce(appManager);
    DataSetManager<Table> outputManager = getDataset("legacyOptOutOutput");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("user1", records.get(0).get("NAME"));
  }

  private void assertDeploymentFailureContains(ApplicationId appId, ETLBatchConfig etlConfig,
                                               String expectedMessageSubstring) throws Exception {
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    try {
      deployApplication(appId, appRequest);
      Assert.fail("Expected deployment failure containing: " + expectedMessageSubstring);
    } catch (Exception e) {
      String message = e.getMessage() == null ? "" : e.getMessage();
      Assert.assertTrue(
        String.format("Expected deployment failure message to contain '%s', but got: '%s'",
                      expectedMessageSubstring, message),
        message.contains(expectedMessageSubstring));
    }
  }

  private int countRows(String table) throws SQLException {
    try (Connection connection = getConnection();
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(String.format("SELECT COUNT(*) FROM \"%s\"", table))) {
      resultSet.next();
      return resultSet.getInt(1);
    }
  }

  private int countRowsWithName(String table, String name) throws SQLException {
    try (Connection connection = getConnection();
         PreparedStatement statement = connection.prepareStatement(
           String.format("SELECT COUNT(*) FROM \"%s\" WHERE NAME = ?", table))) {
      statement.setString(1, name);
      try (ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        return resultSet.getInt(1);
      }
    }
  }

  @Test
  public void testNonExistentDBTable() throws Exception {
    // source
    String importQuery = "SELECT ID, NAME FROM dummy WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) FROM dummy";
    String splitBy = "ID";
    //TODO: Also test for bad connection:
    ETLPlugin sinkConfig = MockSink.getPlugin("table");
    ETLPlugin sourceBadNameConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, getConnectionURL())
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
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
    assertDeployAppFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT);

    // Bad connection
    String badConnection = String.format("jdbc:hsqldb:hsql://localhost/%sWRONG", getDatabase());
    ETLPlugin sourceBadConnConfig = new ETLPlugin(
      DatabaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.CONNECTION_STRING, badConnection)
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "HSQLDBTest")
        .build(),
      null);
    ETLStage sourceBadConn = new ETLStage("sourceBadConn", sourceBadConnConfig);
    etlConfig = ETLBatchConfig.builder()
      .addStage(sourceBadConn)
      .addStage(sink)
      .addConnection(sourceBadConn.getName(), sink.getName())
      .build();
    assertDeployAppFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT);
  }
}

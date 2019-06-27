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

package io.cdap.plugin.db2;

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
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Db2SourceTestRun extends Db2PluginTestBase {

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
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
      .put(Constants.Reference.REFERENCE_NAME, "DBTestSource").build();

    ETLPlugin sourceConfig = new ETLPlugin(
      Db2Constants.PLUGIN_NAME,
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
    String importQuery = "SELECT SMALLINT_COL, INTEGER_COL, BIGINT_COL, DECIMAL_COL, NUMERIC_COL, " +
      " REAL_COL, DOUBLE_COL, CHAR_COL, DECFLOAT_COL, VARCHAR_COL, CHAR_BIT_COL, VARCHAR_BIT_COL, GRAPHIC_COL, " +
      " CLOB_COL, BLOB_COL, DATE_COL, TIME_COL, TIMESTAMP_COL FROM my_table " +
      " WHERE SMALLINT_COL < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(SMALLINT_COL),MAX(SMALLINT_COL) from my_table";
    String splitBy = "SMALLINT_COL";
    ETLPlugin sourceConfig = new ETLPlugin(
      Db2Constants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
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

    // Verify data
    Assert.assertEquals(1, (int) row1.get("SMALLINT_COL"));
    Assert.assertEquals(2, (int) row2.get("SMALLINT_COL"));
    Assert.assertEquals(1, (int) row1.get("INTEGER_COL"));
    Assert.assertEquals(2, (int) row2.get("INTEGER_COL"));
    Assert.assertEquals(1, (long) row1.get("BIGINT_COL"));
    Assert.assertEquals(2, (long) row2.get("BIGINT_COL"));
    Assert.assertEquals(new BigDecimal(4.14, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("DECIMAL_COL"));
    Assert.assertEquals(new BigDecimal(5.14, new MathContext(PRECISION)).setScale(SCALE),
                        row2.getDecimal("DECIMAL_COL"));
    Assert.assertEquals(new BigDecimal(4.14, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("NUMERIC_COL"));
    Assert.assertEquals(new BigDecimal(5.14, new MathContext(PRECISION)).setScale(SCALE),
                        row2.getDecimal("NUMERIC_COL"));
    Assert.assertEquals(4.14, row1.get("DECFLOAT_COL"), 0.00001);
    Assert.assertEquals(5.14, row2.get("DECFLOAT_COL"), 0.00001);
    Assert.assertEquals(4.14f, row1.get("REAL_COL"), 0.00001f);
    Assert.assertEquals(5.14f, row2.get("REAL_COL"), 0.00001f);
    Assert.assertEquals(4.14, row1.get("DOUBLE_COL"), 0.00001);
    Assert.assertEquals(5.14, row2.get("DOUBLE_COL"), 0.00001);
    Assert.assertEquals("user1", row1.get("CHAR_COL").toString().trim());
    Assert.assertEquals("user2", row2.get("CHAR_COL").toString().trim());
    Assert.assertEquals("user1", row1.get("VARCHAR_COL"));
    Assert.assertEquals("user2", row2.get("VARCHAR_COL"));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("CHAR_BIT_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("CHAR_BIT_COL")).array(), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("VARCHAR_BIT_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("VARCHAR_BIT_COL")).array(), 0, 5));
    Assert.assertEquals("user1", row1.get("GRAPHIC_COL").toString().trim());
    Assert.assertEquals("user2", row2.get("GRAPHIC_COL").toString().trim());

    // Verify time columns
    java.util.Date date = new java.util.Date(CURRENT_TS);
    LocalDate expectedDate = date.toInstant()
      .atZone(ZoneId.systemDefault())
      .toLocalDate();

    ZonedDateTime expectedTs = date.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    Assert.assertEquals(expectedDate, row1.getDate("DATE_COL"));
    Assert.assertEquals(expectedTs.toLocalTime().withNano(0), row1.getTime("TIME_COL"));
    Assert.assertEquals(expectedTs, row1.getTimestamp("TIMESTAMP_COL", ZoneId.ofOffset("UTC", ZoneOffset.UTC)));

    // verify binary columns
    Assert.assertEquals("user1", row1.get("CLOB_COL"));
    Assert.assertEquals("user2", row2.get("CLOB_COL"));
    Assert.assertEquals("user1", Bytes.toString((ByteBuffer) row1.get("BLOB_COL")));
    Assert.assertEquals("user2", Bytes.toString((ByteBuffer) row2.get("BLOB_COL")));
  }

  @Test
  public void testDbSourceMultipleTables() throws Exception {
    String importQuery = "SELECT my_table.SMALLINT_COL, your_table.VARCHAR_COL FROM my_table, your_table " +
      "WHERE my_table.SMALLINT_COL < 3 and my_table.SMALLINT_COL = your_table.SMALLINT_COL and $CONDITIONS ";
    String boundingQuery = "SELECT LEAST(MIN(my_table.SMALLINT_COL), MIN(your_table.SMALLINT_COL)), " +
      "GREATEST(MAX(my_table.SMALLINT_COL), MAX(your_table.SMALLINT_COL))";
    String splitBy = "my_table.SMALLINT_COL";
    ETLPlugin sourceConfig = new ETLPlugin(
      Db2Constants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
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
    Assert.assertEquals(1, (int) row1.get("SMALLINT_COL"));
    Assert.assertEquals(2, (int) row2.get("SMALLINT_COL"));
  }

  @Test
  public void testUserNamePasswordCombinations() throws Exception {
    String importQuery = "SELECT * FROM my_table WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(SMALLINT_COL),MAX(SMALLINT_COL) from my_table";
    String splitBy = "SMALLINT_COL";

    ETLPlugin sinkConfig = MockSink.getPlugin("outputTable");

    Map<String, String> baseSourceProps = ImmutableMap.<String, String>builder()
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
    ETLPlugin dbConfig = new ETLPlugin(Db2Constants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, baseSourceProps, null);
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
    database = new ETLStage("databaseSource", new ETLPlugin(Db2Constants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
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
    database = new ETLStage("databaseSource", new ETLPlugin(Db2Constants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
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
      Db2Constants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
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
      Db2Constants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ConnectionConfig.HOST, BASE_PROPS.get(ConnectionConfig.HOST))
        .put(ConnectionConfig.PORT, BASE_PROPS.get(ConnectionConfig.PORT))
        .put(ConnectionConfig.DATABASE, "dumDB")
        .put(ConnectionConfig.USER, BASE_PROPS.get(ConnectionConfig.USER))
        .put(ConnectionConfig.PASSWORD, BASE_PROPS.get(ConnectionConfig.PASSWORD))
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DB2Test")
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

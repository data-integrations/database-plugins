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

package io.cdap.plugin.mssql;

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
import io.cdap.plugin.db.source.AbstractDBSource;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlServerSourceTestRun extends SqlServerPluginTestBase {

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBMacroSupport() throws Exception {
    String importQuery = "SELECT * FROM my_table WHERE DATE_COL <= '${logicalStartTime(yyyy-MM-dd,1d)}' " +
      "AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from my_table";
    String splitBy = "ID";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .putAll(BASE_PROPS)
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
      .put(Constants.Reference.REFERENCE_NAME, "DBTestSource").build();

    ETLPlugin sourceConfig = new ETLPlugin(
      SqlServerConstants.PLUGIN_NAME,
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
    String importQuery = "SELECT ID, NAME, TINY, SMALL, BIG, FLOAT_COL, " +
      "REAL_COL, NUMERIC_COL, CHAR_COL, DECIMAL_COL, BIT_COL, BINARY_COL, DATE_COL, TIME_COL, DATETIME_COL, " +
      "DATETIME2_COL, DATETIMEOFFSET_COL, VARBINARY_COL, VARBINARY_MAX_COL, IMAGE_COL, TEXT_COL, MONEY_COL, " +
      "SMALLMONEY_COL, NCHAR_COL, NTEXT_COL, VARCHAR_MAX_COL, NVARCHAR_COL, NVARCHAR_MAX_COL, SMALLDATETIME_COL, " +
      "TIMESTAMP_COL, UNIQUEIDENTIFIER_COL, XML_COL, SQL_VARIANT_COL, GEOMETRY_COL, GEOGRAPHY_COL, UDT_COL, " +
      "BIG_UDT_COL FROM my_table WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from my_table";
    String splitBy = "ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      SqlServerConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBSourceTest")
        .put(SqlServerConstants.CONNECT_TIMEOUT, "20")
        .put(SqlServerConstants.COLUMN_ENCRYPTION, SqlServerConstants.COLUMN_ENCRYPTION_ENABLED)
        .put(SqlServerConstants.ENCRYPT, "true")
        .put(SqlServerConstants.TRUST_SERVER_CERTIFICATE, "true")
        .put(SqlServerConstants.WORKSTATION_ID, "workstation-1")
        .put(SqlServerConstants.FAILOVER_PARTNER, "localhost")
        .put(SqlServerConstants.PACKET_SIZE, "-1")
        .put(SqlServerConstants.CURRENT_LANGUAGE, "us_english")
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
    String userid = outputRecords.get(0).get("NAME");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    Assert.assertEquals("user1", row1.get("NAME"));
    Assert.assertEquals("user2", row2.get("NAME"));
    Assert.assertEquals("user1", row1.get("TEXT_COL"));
    Assert.assertEquals("user2", row2.get("TEXT_COL"));
    Assert.assertEquals("12345678910", row1.get("UDT_COL"));
    Assert.assertEquals("12345678910", row2.get("UDT_COL"));
    Assert.assertEquals(15417543010L, (long) row1.get("BIG_UDT_COL"));
    Assert.assertEquals(15417543010L, (long) row2.get("BIG_UDT_COL"));
    Assert.assertEquals("char1", ((String) row1.get("CHAR_COL")).trim());
    Assert.assertEquals("char2", ((String) row2.get("CHAR_COL")).trim());
    // trim since 'NCHAR' is fixed-length datatype and result string will contain multiple whitespace chars at the end
    Assert.assertEquals("user1", row1.<String>get("NCHAR_COL").trim());
    Assert.assertEquals("user2", row2.<String>get("NCHAR_COL").trim());
    // trim since 'NTEXT' is fixed-length datatype and result string will contain multiple whitespace chars at the end
    Assert.assertEquals("user1", row1.<String>get("NTEXT_COL").trim());
    Assert.assertEquals("user2", row2.<String>get("NTEXT_COL").trim());

    Assert.assertEquals("user1", row1.get("NVARCHAR_COL"));
    Assert.assertEquals("user2", row2.get("NVARCHAR_COL"));
    Assert.assertEquals("user1", row1.get("VARCHAR_MAX_COL"));
    Assert.assertEquals("user2", row2.get("VARCHAR_MAX_COL"));
    Assert.assertEquals("user1", row1.get("NVARCHAR_MAX_COL"));
    Assert.assertEquals("user2", row2.get("NVARCHAR_MAX_COL"));
    Assert.assertEquals("0E984725-C51C-4BF4-9960-E1C80E27ABA" + 1, row1.get("UNIQUEIDENTIFIER_COL"));
    Assert.assertEquals("0E984725-C51C-4BF4-9960-E1C80E27ABA" + 2, row2.get("UNIQUEIDENTIFIER_COL"));
    Assert.assertEquals("<root><child/></root>", row1.get("XML_COL"));
    Assert.assertEquals("<root><child/></root>", row2.get("XML_COL"));
    Assert.assertEquals("user1", row1.get("SQL_VARIANT_COL"));
    Assert.assertEquals("user2", row2.get("SQL_VARIANT_COL"));

    Assert.assertEquals(1, (int) row1.get("TINY"));
    Assert.assertEquals(2, (int) row2.get("TINY"));
    Assert.assertEquals(1, (int) row1.get("SMALL"));
    Assert.assertEquals(2, (int) row2.get("SMALL"));
    Assert.assertEquals(1, (long) row1.get("BIG"));
    Assert.assertEquals(2, (long) row2.get("BIG"));

    Assert.assertEquals(124.45, (double) row1.get("FLOAT_COL"), 0.00001);
    Assert.assertEquals(125.45, (double) row2.get("FLOAT_COL"), 0.00001);
    Assert.assertEquals(124.45, (float) row1.get("REAL_COL"), 0.00001);
    Assert.assertEquals(125.45, (float) row2.get("REAL_COL"), 0.00001);
    Assert.assertEquals(new BigDecimal(124.45, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("NUMERIC_COL"));
    Assert.assertEquals(new BigDecimal(125.45, new MathContext(PRECISION)).setScale(SCALE),
                        row2.getDecimal("NUMERIC_COL"));
    Assert.assertEquals(new BigDecimal(124.45, new MathContext(PRECISION)).setScale(SCALE),
                        row1.getDecimal("DECIMAL_COL"));
    Assert.assertNull(row2.getDecimal("DECIMAL_COL"));
    Assert.assertFalse(row1.get("BIT_COL"));
    Assert.assertTrue(row2.get("BIT_COL"));
    // Verify time columns
    java.util.Date date = new java.util.Date(CURRENT_TS);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    LocalDate expectedDate = Date.valueOf(sdf.format(date)).toLocalDate();
    ZonedDateTime expectedTs = date.toInstant().atZone(UTC);
    Assert.assertEquals(expectedDate, row1.getDate("DATE_COL"));
    Assert.assertEquals(TIME_MICROS, row1.getTime("TIME_COL"));
    // datetime values are rounded to increments of .000, .003, or .007 seconds
    Assert.assertEquals(expectedTs.toEpochSecond(), row1.getTimestamp("DATETIME_COL", UTC).toEpochSecond());
    Assert.assertEquals(expectedTs, row1.getTimestamp("DATETIME2_COL", UTC));
    Assert.assertEquals("2019-06-24 16:19:15.8010000 +03:00", row1.get("DATETIMEOFFSET_COL"));
    // smalldatetime does not store seconds, minutes can be rounded
    long actualSeconds = row1.getTimestamp("SMALLDATETIME_COL", UTC).toEpochSecond();
    Assert.assertEquals(expectedTs.toEpochSecond() / (60 * 60), actualSeconds / (60 * 60));

    // verify binary columns
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("BINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("BINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("VARBINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("VARBINARY_COL")).array(), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("VARBINARY_MAX_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("VARBINARY_MAX_COL")).array(), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(((ByteBuffer) row1.get("IMAGE_COL")).array(), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(((ByteBuffer) row2.get("IMAGE_COL")).array(), 0, 5));
    Assert.assertEquals(new BigDecimal(125.45, new MathContext(MONEY_PRECISION))
                          .setScale(MONEY_SCALE, BigDecimal.ROUND_HALF_UP),
                        row2.getDecimal("MONEY_COL"));
    Assert.assertEquals(new BigDecimal(125.45, new MathContext(SMALL_MONEY_PRECISION))
                          .setScale(SMALL_MONEY_SCALE, BigDecimal.ROUND_HALF_UP),
                        row2.getDecimal("SMALLMONEY_COL"));
    Assert.assertArrayEquals(Bytes.getBytes(TIMESTAMP_VALUES.get(1)), Bytes.getBytes(row2.get("TIMESTAMP_COL")));
    Assert.assertArrayEquals(Bytes.getBytes(GEOMETRY_VALUES.get(1)), Bytes.getBytes(row2.get("GEOMETRY_COL")));
    Assert.assertArrayEquals(Bytes.getBytes(GEOGRAPHY_VALUES.get(1)), Bytes.getBytes(row2.get("GEOGRAPHY_COL")));
  }

  @Test
  public void testDbSourceMultipleTables() throws Exception {
    String importQuery = "SELECT my_table.ID, your_table.NAME FROM my_table, your_table " +
      "WHERE my_table.ID < 3 and my_table.ID = your_table.ID and $CONDITIONS ";
    String boundingQuery = "SELECT LEAST(MIN(my_table.ID), MIN(your_table.ID)), " +
      "GREATEST(MAX(my_table.ID), MAX(your_table.ID))";
    String splitBy = "my_table.ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      SqlServerConstants.PLUGIN_NAME,
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
    String importQuery = "SELECT * FROM my_table WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from my_table";
    String splitBy = "ID";

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
      .put(SqlServerConstants.CONNECT_TIMEOUT, "20")
      .put(SqlServerConstants.COLUMN_ENCRYPTION, SqlServerConstants.COLUMN_ENCRYPTION_ENABLED)
      .put(SqlServerConstants.ENCRYPT, "true")
      .put(SqlServerConstants.TRUST_SERVER_CERTIFICATE, "true")
      .put(SqlServerConstants.WORKSTATION_ID, "workstation-1")
      .put(SqlServerConstants.FAILOVER_PARTNER, "localhost")
      .put(SqlServerConstants.PACKET_SIZE, "-1")
      .put(SqlServerConstants.CURRENT_LANGUAGE, "us_english")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("dbTest");

    // null user name, null password. Should succeed.
    // as source
    ETLPlugin dbConfig = new ETLPlugin(SqlServerConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, baseSourceProps, null);
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
                            new ETLPlugin(SqlServerConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, noUser, null));
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
    database = new ETLStage("databaseSource", new ETLPlugin(SqlServerConstants.PLUGIN_NAME,
                                                            BatchSource.PLUGIN_TYPE, emptyPassword, null));
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
      SqlServerConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBNonExistentTest")
        .put(SqlServerConstants.CONNECT_TIMEOUT, "20")
        .put(SqlServerConstants.COLUMN_ENCRYPTION, SqlServerConstants.COLUMN_ENCRYPTION_ENABLED)
        .put(SqlServerConstants.ENCRYPT, "true")
        .put(SqlServerConstants.TRUST_SERVER_CERTIFICATE, "true")
        .put(SqlServerConstants.WORKSTATION_ID, "workstation-1")
        .put(SqlServerConstants.FAILOVER_PARTNER, "localhost")
        .put(SqlServerConstants.PACKET_SIZE, "-1")
        .put(SqlServerConstants.CURRENT_LANGUAGE, "us_english")
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
    ETLPlugin sourceBadConnConfig = new ETLPlugin(
      SqlServerConstants.PLUGIN_NAME,
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
        .put(Constants.Reference.REFERENCE_NAME, "SQLServerTest")
        .put(SqlServerConstants.CONNECT_TIMEOUT, "20")
        .put(SqlServerConstants.COLUMN_ENCRYPTION, SqlServerConstants.COLUMN_ENCRYPTION_ENABLED)
        .put(SqlServerConstants.ENCRYPT, "true")
        .put(SqlServerConstants.TRUST_SERVER_CERTIFICATE, "true")
        .put(SqlServerConstants.WORKSTATION_ID, "workstation-1")
        .put(SqlServerConstants.FAILOVER_PARTNER, "localhost")
        .put(SqlServerConstants.PACKET_SIZE, "-1")
        .put(SqlServerConstants.CURRENT_LANGUAGE, "us_english")
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

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

package io.cdap.plugin.mysql;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.CustomAssertions;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for ETL using databases.
 */
public class MysqlSinkTestRun extends MysqlPluginTestBase {

  private static final Schema SCHEMA = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
    Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("SCORE", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("GRADUATED", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("TINY", Schema.of(Schema.Type.INT)),
    Schema.Field.of("SMALL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("BIG", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("MEDIUMINT_COL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("FLOAT_COL", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("REAL_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("NUMERIC_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("BIT_COL", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("DATETIME_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("YEAR_COL", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("TEXT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TINYTEXT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("MEDIUMTEXT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("LONGTEXT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("BINARY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("VARBINARY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("TINYBLOB_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("BLOB_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("MEDIUMBLOB_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("LONGBLOB_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("ENUM_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("SET_COL", Schema.of(Schema.Type.STRING))
  );

  @Before
  public void setup() throws Exception {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute("TRUNCATE TABLE MY_DEST_TABLE");
    }
  }

  @Test
  public void testDBSinkWithInvalidFieldType() throws Exception {
    testDBInvalidFieldType("ID", Schema.Type.STRING, getSinkConfig(), DATAPIPELINE_ARTIFACT);
  }

  @Test
  public void testDBSinkWithInvalidFieldLogicalType() throws Exception {
    testDBInvalidFieldLogicalType("TIMESTAMP_COL", Schema.Type.LONG, getSinkConfig(), DATAPIPELINE_ARTIFACT);
  }

  @Test
  public void testDBSinkWithDBSchemaAndInvalidData() throws Exception {
    // MySQL JDBC connector allows to write integer values to STRING column. Use ENUM column instead.
    String enumColumnName = "ENUM_COL";
    startPipelineAndWriteInvalidData(enumColumnName, getSinkConfig(), DATAPIPELINE_ARTIFACT);
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE")) {
      testInvalidDataWrite(resultSet, enumColumnName);
    }
  }

  @Test
  public void testDBSinkWithExplicitInputSchema() throws Exception {
    testDBSink("testDBSinkWithExplicitInputSchema", "input-dbsinktest-explicit", SCHEMA);
  }

  @Test
  public void testDBSinkWithInferredInputSchema() throws Exception {
    testDBSink("testDBSinkWithInferredInputSchema", "input-dbsinktest-inferred", null);
  }

  private void testDBSink(String appName, String inputDatasetName, Schema schema) throws Exception {
    ETLPlugin sourceConfig = (schema != null)
      ? MockSource.getPlugin(inputDatasetName, schema)
      : MockSource.getPlugin(inputDatasetName);

    ETLPlugin sinkConfig = getSinkConfig();

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, appName);

    // Prepare test input data
    List<StructuredRecord> inputRecords = createInputData();
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, inputRecords);
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet actual = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE ORDER BY ID")) {

      for (StructuredRecord expected : inputRecords) {
        Assert.assertTrue(actual.next());

        // Verify data
        CustomAssertions.assertObjectEquals(expected.get("ID"), actual.getInt("ID"));
        CustomAssertions.assertObjectEquals(expected.get("NAME"), actual.getString("NAME"));
        CustomAssertions.assertObjectEquals(expected.get("TEXT_COL"), actual.getString("TEXT_COL"));
        CustomAssertions.assertObjectEquals(expected.get("TINYTEXT_COL"), actual.getString("TINYTEXT_COL"));
        CustomAssertions.assertObjectEquals(expected.get("MEDIUMTEXT_COL"), actual.getString("MEDIUMTEXT_COL"));
        CustomAssertions.assertObjectEquals(expected.get("LONGTEXT_COL"), actual.getString("LONGTEXT_COL"));
        CustomAssertions.assertObjectEquals(expected.get("CHAR_COL"), actual.getString("CHAR_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("GRADUATED"), actual.getBoolean("GRADUATED"));
        Assert.assertNull(actual.getString("NOT_IMPORTED"));
        CustomAssertions.assertObjectEquals(expected.get("ENUM_COL"), actual.getString("ENUM_COL"));
        CustomAssertions.assertObjectEquals(expected.get("SET_COL"), actual.getString("SET_COL"));
        CustomAssertions.assertObjectEquals(expected.get("TINY"), actual.getInt("TINY"));
        CustomAssertions.assertObjectEquals(expected.get("SMALL"), actual.getInt("SMALL"));
        CustomAssertions.assertObjectEquals(expected.get("BIG"), actual.getLong("BIG"));
        CustomAssertions.assertObjectEquals(expected.get("MEDIUMINT_COL"), actual.getInt("MEDIUMINT_COL"));
        CustomAssertions.assertNumericEquals(expected.get("SCORE"), actual.getDouble("SCORE"));
        CustomAssertions.assertNumericEquals(expected.get("FLOAT_COL"), actual.getFloat("FLOAT_COL"));
        CustomAssertions.assertNumericEquals(expected.get("REAL_COL"), actual.getDouble("REAL_COL"));
        CustomAssertions.assertObjectEquals(expected.getDecimal("NUMERIC_COL"), actual.getBigDecimal("NUMERIC_COL"));
        CustomAssertions.assertObjectEquals(expected.getDecimal("DECIMAL_COL"), actual.getBigDecimal("DECIMAL_COL"));
        CustomAssertions.assertObjectEquals(expected.get("BIT_COL"), actual.getBoolean("BIT_COL"));

        // Verify binary columns
        Assert.assertArrayEquals(expected.get("BINARY_COL"), actual.getBytes("BINARY_COL"));
        Assert.assertArrayEquals(expected.get("VARBINARY_COL"), actual.getBytes("VARBINARY_COL"));
        Assert.assertArrayEquals(expected.get("BLOB_COL"), actual.getBytes("BLOB_COL"));
        Assert.assertArrayEquals(expected.get("MEDIUMBLOB_COL"), actual.getBytes("MEDIUMBLOB_COL"));
        Assert.assertArrayEquals(expected.get("TINYBLOB_COL"), actual.getBytes("TINYBLOB_COL"));
        Assert.assertArrayEquals(expected.get("LONGBLOB_COL"), actual.getBytes("LONGBLOB_COL"));

        // Verify time columns
        Assert.assertEquals(expected.getDate("DATE_COL"), actual.getDate("DATE_COL").toLocalDate());

        // compare seconds, since mysql 'time' type does not store milliseconds but 'LocalTime' does
        Assert.assertEquals(expected.getTime("TIME_COL").toSecondOfDay(),
                            actual.getTime("TIME_COL").toLocalTime().toSecondOfDay());
        Assert.assertEquals(expected.getDate("YEAR_COL").getYear(), actual.getInt("YEAR_COL"));
        Assert.assertEquals(expected.getTimestamp("DATETIME_COL"),
                            actual.getTimestamp("DATETIME_COL").toInstant().atZone(UTC_ZONE));
        Assert.assertEquals(expected.getTimestamp("TIMESTAMP_COL"),
                            actual.getTimestamp("TIMESTAMP_COL").toInstant().atZone(UTC_ZONE));
      }
    }
  }

  private List<StructuredRecord> createInputData() throws Exception {
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      StructuredRecord.Builder builder = StructuredRecord.builder(SCHEMA)
        .set("ID", i)
        .set("NAME", name)
        .set("SCORE", 3.451)
        .set("GRADUATED", (i % 2 == 0))
        .set("TINY", i)
        .set("SMALL", i)
        .set("BIG", 3456987L)
        .set("MEDIUMINT_COL", 8388607)
        .set("FLOAT_COL", 3.456f)
        .set("REAL_COL", 3.457)
        .setDecimal("NUMERIC_COL", new BigDecimal(3.458d, new MathContext(PRECISION)).setScale(SCALE))
        .setDecimal("DECIMAL_COL", new BigDecimal(3.459d, new MathContext(PRECISION)).setScale(SCALE))
        .set("BIT_COL", (i % 2 == 1))
        .setDate("DATE_COL", localDateTime.toLocalDate())
        .setTime("TIME_COL", localDateTime.toLocalTime())
        .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(UTC_ZONE))
        .setTimestamp("DATETIME_COL",  localDateTime.atZone(UTC_ZONE))
        .setDate("YEAR_COL",  localDateTime.toLocalDate())
        .set("TEXT_COL", name)
        .set("TINYTEXT_COL", name)
        .set("MEDIUMTEXT_COL", name)
        .set("LONGTEXT_COL", name)
        .set("CHAR_COL", "char" + i)
        .set("BINARY_COL", name.getBytes(Charsets.UTF_8))
        .set("VARBINARY_COL", name.getBytes(Charsets.UTF_8))
        .set("TINYBLOB_COL", name.getBytes(Charsets.UTF_8))
        .set("BLOB_COL", name.getBytes(Charsets.UTF_8))
        .set("MEDIUMBLOB_COL", name.getBytes(Charsets.UTF_8))
        .set("LONGBLOB_COL", name.getBytes(Charsets.UTF_8))
        .set("ENUM_COL", "Second")
        .set("SET_COL", "a,b,c,d");

      inputRecords.add(builder.build());
    }

    return inputRecords;
  }

  private ETLPlugin getSinkConfig() {
    return new ETLPlugin(
      MysqlConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(MysqlConstants.AUTO_RECONNECT, "true")
        .put(MysqlConstants.USE_COMPRESSION, "true")
        .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);
  }
}

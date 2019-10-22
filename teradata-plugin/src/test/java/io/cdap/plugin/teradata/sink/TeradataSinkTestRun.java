/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.teradata.sink;

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
import io.cdap.plugin.teradata.TeradataConstants;
import io.cdap.plugin.teradata.TeradataPluginTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class TeradataSinkTestRun extends TeradataPluginTestBase {

  private static final Schema SCHEMA = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
    Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("SCORE", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("GRADUATED", Schema.of(Schema.Type.INT)),
    Schema.Field.of("SMALL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("BIG", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("NUMBER_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("TIMETZ_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("TIMESTAMPTZ_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("VARCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("BINARY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("VARBINARY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("BLOB_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("INTERVAL_YEAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_YEAR_TO_MONTH_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_MONTH_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_DAY_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_DAY_TO_HOUR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_DAY_TO_MINUTE_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_DAY_TO_SECOND_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_HOUR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_HOUR_TO_MINUTE_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_HOUR_TO_SECOND_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_MINUTE_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_MINUTE_TO_SECOND_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_SECOND_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("ST_GEOMETRY_COL", Schema.of(Schema.Type.STRING))
  );

  @Before
  public void setup() throws Exception {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute("DELETE MY_DEST_TABLE ALL");
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
        CustomAssertions.assertObjectEquals(expected.get("CHAR_COL"), actual.getString("CHAR_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("VARCHAR_COL"),
                                            actual.getString("VARCHAR_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("GRADUATED"), actual.getInt("GRADUATED"));
        Assert.assertNull(actual.getString("NOT_IMPORTED"));
        CustomAssertions.assertObjectEquals(expected.get("SMALL"), actual.getInt("SMALL"));
        CustomAssertions.assertObjectEquals(expected.get("BIG"), actual.getLong("BIG"));
        CustomAssertions.assertNumericEquals(expected.get("SCORE"), actual.getDouble("SCORE"));
        CustomAssertions.assertObjectEquals(expected.getDecimal("NUMBER_COL"),
                                            actual.getBigDecimal("NUMBER_COL")
                                              .setScale(SCALE, RoundingMode.HALF_EVEN));
        CustomAssertions.assertObjectEquals(expected.getDecimal("DECIMAL_COL"),
                                            actual.getBigDecimal("DECIMAL_COL"));

        Clob clob = actual.getClob("ST_GEOMETRY_COL");
        CustomAssertions.assertObjectEquals(expected.get("ST_GEOMETRY_COL"),
                                            clob.getSubString(1, (int) clob.length()));

        // Verify interval columns
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_YEAR_COL"),
                                            actual.getString("INTERVAL_YEAR_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_YEAR_TO_MONTH_COL"),
                                            actual.getString("INTERVAL_YEAR_TO_MONTH_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_MONTH_COL"),
                                            actual.getString("INTERVAL_MONTH_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_DAY_COL"),
                                            actual.getString("INTERVAL_DAY_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_DAY_TO_HOUR_COL"),
                                            actual.getString("INTERVAL_DAY_TO_HOUR_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_DAY_TO_MINUTE_COL"),
                                            actual.getString("INTERVAL_DAY_TO_MINUTE_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_DAY_TO_SECOND_COL"),
                                            actual.getString("INTERVAL_DAY_TO_SECOND_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_HOUR_COL"),
                                            actual.getString("INTERVAL_HOUR_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_HOUR_TO_MINUTE_COL"),
                                            actual.getString("INTERVAL_HOUR_TO_MINUTE_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_HOUR_TO_SECOND_COL"),
                                            actual.getString("INTERVAL_HOUR_TO_SECOND_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_MINUTE_COL"),
                                            actual.getString("INTERVAL_MINUTE_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_MINUTE_TO_SECOND_COL"),
                                            actual.getString("INTERVAL_MINUTE_TO_SECOND_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("INTERVAL_SECOND_COL"),
                                            actual.getString("INTERVAL_SECOND_COL").trim());

        // Verify binary columns
        Assert.assertArrayEquals(expected.get("BINARY_COL"), actual.getBytes("BINARY_COL"));
        Assert.assertArrayEquals(expected.get("VARBINARY_COL"), actual.getBytes("VARBINARY_COL"));
        Assert.assertArrayEquals(expected.get("BLOB_COL"), actual.getBytes("BLOB_COL"));

        // Verify time columns
        Assert.assertEquals(expected.getDate("DATE_COL"),
                            actual.getDate("DATE_COL").toLocalDate());

        // compare seconds, since mysql 'time' type does not store milliseconds but 'LocalTime' does
        Assert.assertEquals(expected.getTime("TIME_COL").toSecondOfDay(),
                            actual.getTime("TIME_COL").toLocalTime().toSecondOfDay());
        Assert.assertEquals(expected.getTimestamp("TIMESTAMP_COL"),
                            actual.getTimestamp("TIMESTAMP_COL").toInstant().atZone(UTC_ZONE));
        Assert.assertEquals(expected.getTime("TIMETZ_COL").toSecondOfDay(),
                            actual.getTime("TIMETZ_COL").toLocalTime().toSecondOfDay());
        Assert.assertEquals(expected.getTimestamp("TIMESTAMPTZ_COL"),
                            actual.getTimestamp("TIMESTAMPTZ_COL").toInstant().atZone(UTC_ZONE));
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
        .set("GRADUATED", (i % 2 == 0) ? 1 : 0)
        .set("SMALL", i)
        .set("BIG", 3456987L)
        .setDecimal("NUMBER_COL", new BigDecimal(3.458d, new MathContext(PRECISION)).setScale(SCALE))
        .setDecimal("DECIMAL_COL", new BigDecimal(3.459d, new MathContext(PRECISION)).setScale(SCALE))
        .setDate("DATE_COL", localDateTime.toLocalDate())
        .setTime("TIME_COL", localDateTime.toLocalTime())
        .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(UTC_ZONE))
        .setTime("TIMETZ_COL", localDateTime.toLocalTime())
        .setTimestamp("TIMESTAMPTZ_COL", localDateTime.atZone(UTC_ZONE))
        .set("CHAR_COL", "char" + i)
        .set("VARCHAR_COL", "char" + i)
        .set("BINARY_COL", name.getBytes(Charsets.UTF_8))
        .set("VARBINARY_COL", name.getBytes(Charsets.UTF_8))
        .set("BLOB_COL", name.getBytes(Charsets.UTF_8))
        .set("INTERVAL_YEAR_COL", "2019")
        .set("INTERVAL_YEAR_TO_MONTH_COL", "2019-10")
        .set("INTERVAL_MONTH_COL", "10")
        .set("INTERVAL_DAY_COL", "11")
        .set("INTERVAL_DAY_TO_HOUR_COL", "11 12")
        .set("INTERVAL_DAY_TO_MINUTE_COL", "11 12:13")
        .set("INTERVAL_DAY_TO_SECOND_COL", "11 12:13:14.567")
        .set("INTERVAL_HOUR_COL", "12")
        .set("INTERVAL_HOUR_TO_MINUTE_COL", "12:13")
        .set("INTERVAL_HOUR_TO_SECOND_COL", "12:13:14.567")
        .set("INTERVAL_MINUTE_COL", "13")
        .set("INTERVAL_MINUTE_TO_SECOND_COL", "13:14.567")
        .set("INTERVAL_SECOND_COL", "14.567")
        .set("ST_GEOMETRY_COL", "POINT (10 20)");

      inputRecords.add(builder.build());
    }

    return inputRecords;
  }

  private ETLPlugin getSinkConfig() {
    return new ETLPlugin(
      TeradataConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);
  }
}

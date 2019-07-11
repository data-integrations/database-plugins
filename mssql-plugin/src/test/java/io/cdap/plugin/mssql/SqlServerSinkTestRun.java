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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
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
public class SqlServerSinkTestRun extends SqlServerPluginTestBase {

  private static final Schema SCHEMA = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
    Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TINY", Schema.of(Schema.Type.INT)),
    Schema.Field.of("SMALL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("BIG", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("FLOAT_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("REAL_COL", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("NUMERIC_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("BIT_COL", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("DATETIME_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("DATETIME2_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("DATETIMEOFFSET_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("SMALLDATETIME_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("BINARY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("VARBINARY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("VARBINARY_MAX_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("IMAGE_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("MONEY_COL", Schema.decimalOf(MONEY_PRECISION, MONEY_SCALE)),
    Schema.Field.of("SMALLMONEY_COL", Schema.decimalOf(SMALL_MONEY_PRECISION, SMALL_MONEY_SCALE)),
    Schema.Field.of("NCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NTEXT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NVARCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NVARCHAR_MAX_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("VARCHAR_MAX_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TEXT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("UNIQUEIDENTIFIER_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("XML_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("SQL_VARIANT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("GEOMETRY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("GEOGRAPHY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("GEOMETRY_WKT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("GEOGRAPHY_WKT_COL", Schema.of(Schema.Type.STRING)),
    // UDT will be mapped as basic type if it's an alias of this type
    // In this case UDT_COL is alias of 'varchar(11)'
    Schema.Field.of("UDT_COL", Schema.of(Schema.Type.STRING)),
    // BIG_UDT_COL is alias of 'bigint'
    Schema.Field.of("BIG_UDT_COL", Schema.of(Schema.Type.LONG))
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
    String stringColumnName = "NAME";
    startPipelineAndWriteInvalidData(stringColumnName, getSinkConfig(), DATAPIPELINE_ARTIFACT);
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE")) {
      testInvalidDataWrite(resultSet, stringColumnName);
    }
  }

  @Test
  public void testDBSinkWithExplicitInputSchema() throws Exception {
    testDBSink("testDBSinkWithExplicitInputSchema", "input-dbsinktest-explicit", SCHEMA);
  }

  @Test
  public void testDBSinkWithInferredSchema() throws Exception {
    testDBSink("testDBSinkWithInferredInputSchema", "input-dbsinktest-inferred", null);
  }

  private void testDBSink(String appName, String inputDatasetName, Schema schema) throws Exception {
    ETLPlugin sourceConfig = (schema != null)
      ? MockSource.getPlugin(inputDatasetName, schema)
      : MockSource.getPlugin(inputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, getSinkConfig(), DATAPIPELINE_ARTIFACT, appName);
    List<StructuredRecord> inputRecords = createInputData();
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, inputRecords);
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet actual = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE ORDER BY ID")) {

      for (StructuredRecord expected: inputRecords) {
        Assert.assertTrue(actual.next());
        CustomAssertions.assertObjectEquals(expected.get("ID"), actual.getInt("ID"));
        CustomAssertions.assertObjectEquals(expected.get("TINY"), actual.getInt("TINY"));
        CustomAssertions.assertObjectEquals(expected.get("SMALL"), actual.getInt("SMALL"));
        CustomAssertions.assertObjectEquals(expected.getDate("DATE_COL"), actual.getDate("DATE_COL").toLocalDate());

        CustomAssertions.assertObjectEquals(expected.getTime("TIME_COL"),
                           actual.getTimestamp("TIME_COL").toLocalDateTime().toLocalTime());
        CustomAssertions.assertObjectEquals(expected.getTimestamp("DATETIME2_COL"),
                           actual.getTimestamp("DATETIME2_COL").toLocalDateTime().atZone(UTC));
        CustomAssertions.assertObjectEquals(expected.get("DATETIMEOFFSET_COL"), actual.getString("DATETIMEOFFSET_COL"));

        // 'datetime' values are rounded to increments of .000, .003, or .007 seconds
        CustomAssertions.assertObjectEquals(expected.getTimestamp("DATETIME_COL").toInstant().toEpochMilli() / 100,
                           actual.getTimestamp("DATETIME_COL").toInstant().toEpochMilli() / 100);

        // 'smalldate' does not store seconds and minutes can be rounded. Compare with 10 min accuracy
        CustomAssertions.assertObjectEquals(expected.getTimestamp("SMALLDATETIME_COL").toEpochSecond() / 600,
                           actual.getTimestamp("SMALLDATETIME_COL").toInstant().getEpochSecond() / 600);

        CustomAssertions.assertObjectEquals(expected.getDecimal("MONEY_COL"), actual.getBigDecimal("MONEY_COL"));
        CustomAssertions.assertObjectEquals(expected.getDecimal("SMALLMONEY_COL"),
                                            actual.getBigDecimal("SMALLMONEY_COL"));
        CustomAssertions.assertObjectEquals(expected.getDecimal("NUMERIC_COL"), actual.getBigDecimal("NUMERIC_COL"));
        CustomAssertions.assertObjectEquals(expected.getDecimal("DECIMAL_COL"), actual.getBigDecimal("DECIMAL_COL"));

        // SQL Server 'float' is 'float(53)' by default, uses 8 bytes
        CustomAssertions.assertNumericEquals(expected.get("FLOAT_COL"), actual.getDouble("FLOAT_COL"));
        CustomAssertions.assertNumericEquals(expected.<Float>get("REAL_COL"), actual.getFloat("REAL_COL"));

        // trim since 'NTEXT' is fixed-length datatype and result string will contain multiple whitespace chars
        // at the end
        CustomAssertions.assertObjectEquals(expected.get("NTEXT_COL"), actual.getString("NTEXT_COL").trim());

        // trim since 'NCHAR' is fixed-length datatype and result string will contain multiple whitespace chars
        // at the end
        CustomAssertions.assertObjectEquals(expected.get("NCHAR_COL"), actual.getString("NCHAR_COL").trim());
        CustomAssertions.assertObjectEquals(expected.get("CHAR_COL"), actual.getString("CHAR_COL").trim());

        CustomAssertions.assertObjectEquals(expected.get("NAME"), actual.getString("NAME"));
        CustomAssertions.assertObjectEquals(expected.get("NVARCHAR_COL"), actual.getString("NVARCHAR_COL"));
        CustomAssertions.assertObjectEquals(expected.get("TEXT_COL"), actual.getString("TEXT_COL"));
        CustomAssertions.assertObjectEquals(expected.get("SQL_VARIANT_COL"), actual.getString("SQL_VARIANT_COL"));
        CustomAssertions.assertObjectEquals(expected.get("XML_COL"), actual.getString("XML_COL"));
        CustomAssertions.assertObjectEquals(expected.get("NVARCHAR_MAX_COL"), actual.getString("NVARCHAR_MAX_COL"));
        CustomAssertions.assertObjectEquals(expected.get("VARCHAR_MAX_COL"), actual.getString("VARCHAR_MAX_COL"));
        CustomAssertions.assertObjectEquals(expected.get("UNIQUEIDENTIFIER_COL"),
                                            actual.getString("UNIQUEIDENTIFIER_COL"));

        Assert.assertArrayEquals(expected.get("BINARY_COL"), actual.getBytes("BINARY_COL"));
        Assert.assertArrayEquals(expected.get("VARBINARY_COL"), actual.getBytes("VARBINARY_COL"));
        Assert.assertArrayEquals(expected.get("VARBINARY_MAX_COL"), actual.getBytes("VARBINARY_MAX_COL"));
        Assert.assertArrayEquals(expected.get("IMAGE_COL"), actual.getBytes("IMAGE_COL"));
        Assert.assertArrayEquals(expected.get("GEOMETRY_COL"), actual.getBytes("GEOMETRY_COL"));
        Assert.assertArrayEquals(expected.get("GEOGRAPHY_COL"), actual.getBytes("GEOGRAPHY_COL"));

        CustomAssertions.assertObjectEquals(expected.get("UDT_COL"), actual.getString("UDT_COL"));
        CustomAssertions.assertObjectEquals(expected.get("BIG_UDT_COL"), actual.getLong("BIG_UDT_COL"));
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
        .set("TINY", i + 1)
        .set("SMALL", i + 2)
        .set("BIG", 3456987L)
        .set("FLOAT_COL", Double.MAX_VALUE)
        .set("REAL_COL", 3.457f)
        .setDecimal("NUMERIC_COL", new BigDecimal(3.458d, new MathContext(PRECISION)).setScale(SCALE))
        .setDecimal("DECIMAL_COL", new BigDecimal(3.459d, new MathContext(PRECISION)).setScale(SCALE))
        .set("BIT_COL", (i % 2 == 1))
        .setDate("DATE_COL", localDateTime.toLocalDate())
        .setTime("TIME_COL", TIME_MICROS)
        .setTimestamp("DATETIME_COL", localDateTime.atZone(UTC))
        .setTimestamp("DATETIME2_COL", localDateTime.atZone(UTC))
        .set("DATETIMEOFFSET_COL", "2019-06-24 16:19:15.8010000 +03:00")
        .setTimestamp("SMALLDATETIME_COL", localDateTime.atZone(UTC))
        .set("BINARY_COL", name.getBytes(Charsets.UTF_8))
        .set("VARBINARY_COL", name.getBytes(Charsets.UTF_8))
        .set("VARBINARY_MAX_COL", name.getBytes(Charsets.UTF_8))
        .set("IMAGE_COL", name.getBytes(Charsets.UTF_8))
        .setDecimal("MONEY_COL", new BigDecimal(123.45, new MathContext(MONEY_PRECISION))
          .setScale(MONEY_SCALE, BigDecimal.ROUND_HALF_UP)
          .add(new BigDecimal(i)))
        .setDecimal("SMALLMONEY_COL", new BigDecimal(123.45, new MathContext(SMALL_MONEY_PRECISION))
          .setScale(SMALL_MONEY_SCALE, BigDecimal.ROUND_HALF_UP)
          .add(new BigDecimal(i)))
        .set("NCHAR_COL", name)
        .set("CHAR_COL", name)
        .set("NTEXT_COL", name)
        .set("NVARCHAR_COL", name)
        .set("NVARCHAR_MAX_COL", name)
        .set("VARCHAR_MAX_COL", name)
        .set("TEXT_COL", name)
        .set("UNIQUEIDENTIFIER_COL", "0E984725-C51C-4BF4-9960-E1C80E27ABA" + i)
        .set("XML_COL", "<root><child/></root>")
        .set("SQL_VARIANT_COL", name)
        .set("GEOMETRY_COL", Bytes.getBytes(GEOMETRY_VALUES.get(i)))
        .set("GEOGRAPHY_COL", Bytes.getBytes(GEOGRAPHY_VALUES.get(i)))
        .set("GEOMETRY_WKT_COL", "POINT(3 40 5 6)")
        .set("GEOGRAPHY_WKT_COL", "POINT(3 40 5 6)")
        .set("UDT_COL", "12345678910")
        .set("BIG_UDT_COL", 15417543010L);
      inputRecords.add(builder.build());
    }
    return inputRecords;
  }

  private ETLPlugin getSinkConfig() {
    return new ETLPlugin(
      SqlServerConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
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
  }
}

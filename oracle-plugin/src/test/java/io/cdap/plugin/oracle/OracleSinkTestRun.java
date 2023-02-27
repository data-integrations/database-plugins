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

import com.google.common.collect.ImmutableList;
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
import io.cdap.plugin.db.sink.AbstractDBSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Test for ETL using databases.
 */
public class OracleSinkTestRun extends OraclePluginTestBase {

  public static final Schema COMMON_FIELDS = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("CHARACTER_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("VARCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("VARCHAR2_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NVARCHAR2_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("INTERVAL_YEAR_TO_MONTH_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_DAY_TO_SECOND_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("RAW_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("CLOB_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NCLOB_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("BLOB_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("FLOAT_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("REAL_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("BINARY_FLOAT_COL", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("BINARY_DOUBLE_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("LONG_RAW_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("ROWID_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("UROWID_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TIMESTAMPTZ_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TIMESTAMPLTZ_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
  );

  /**
   * Since all Oracle numeric types are based on NUMBER(i.e. INTEGER type is actually NUMBER(38, 0)),
   * the exact schema uses decimal logical type for INTEGER, INT, SMALLINT, DEC, DECIMAL, NUMBER.
   */
  public static final Schema DECIMAL_SCHEMA = Schema.recordOf(
    "dbRecord",
    ImmutableList.<Schema.Field>builder()
      .addAll(COMMON_FIELDS.getFields())
      .add(Schema.Field.of("ID", Schema.decimalOf(DEFAULT_PRECISION)))
      .add(Schema.Field.of("INT_COL", Schema.decimalOf(DEFAULT_PRECISION)))
      .add(Schema.Field.of("INTEGER_COL", Schema.decimalOf(DEFAULT_PRECISION)))
      .add(Schema.Field.of("DEC_COL", Schema.decimalOf(DEFAULT_PRECISION)))
      .add(Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)))
      .add(Schema.Field.of("NUMBER_COL", Schema.decimalOf(PRECISION, SCALE)))
      .add(Schema.Field.of("NUMERIC_COL", Schema.decimalOf(PRECISION, SCALE)))
      .add(Schema.Field.of("SMALLINT_COL", Schema.decimalOf(DEFAULT_PRECISION)))
      .build()
  );

  /**
   * It's safe to write primitives as values of decimal logical type in the case of valid precision.
   * Thus the following schema compatibility is supported:
   * 1) Schema.Type.INT -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 10)
   * 2) Schema.Type.LONG -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 19)
   * 3) Schema.Type.FLOAT -> Schema.LogicalType.DECIMAL (primitive value can be rounded to match actual schema)
   * 4) Schema.Type.DOUBLE -> Schema.LogicalType.DECIMAL (primitive value can be rounded to match actual schema)
   */
  public static final Schema PRIMITIVE_SCHEMA = Schema.recordOf(
    "dbRecord",
    ImmutableList.<Schema.Field>builder()
      .addAll(COMMON_FIELDS.getFields())
      .add(Schema.Field.of("ID", Schema.of(Schema.Type.INT)))
      .add(Schema.Field.of("INT_COL", Schema.of(Schema.Type.INT)))
      .add(Schema.Field.of("INTEGER_COL", Schema.of(Schema.Type.INT)))
      .add(Schema.Field.of("DEC_COL", Schema.of(Schema.Type.LONG)))
      .add(Schema.Field.of("SMALLINT_COL", Schema.of(Schema.Type.LONG)))
      .build()
  );

  public static final Schema LONG_COLUMN_SCHEMA = Schema.recordOf(
    "dbRecord",
    ImmutableList.<Schema.Field>builder()
      .add(Schema.Field.of("ID", Schema.of(Schema.Type.INT)))
      .add(Schema.Field.of("SMALLINT_COL", Schema.of(Schema.Type.LONG)))
      .add(Schema.Field.of("VARCHAR_COL", Schema.of(Schema.Type.STRING)))
      .add(Schema.Field.of("LONG_COL", Schema.of(Schema.Type.STRING)))
      .build()
  );

  private static final BiConsumer<StructuredRecord, ResultSet> COMPARE_COMMON = (expected, actual) -> {
    try {
      // Verify data
      Assert.assertArrayEquals(expected.get("LONG_RAW_COL"), actual.getBytes("LONG_RAW_COL"));

      CustomAssertions.assertNumericEquals(expected.get("FLOAT_COL"), actual.getDouble("FLOAT_COL"));
      CustomAssertions.assertNumericEquals(expected.get("REAL_COL"), actual.getDouble("REAL_COL"));
      CustomAssertions.assertNumericEquals(expected.get("BINARY_FLOAT_COL"), actual.getFloat("BINARY_FLOAT_COL"));
      CustomAssertions.assertNumericEquals(expected.get("BINARY_DOUBLE_COL"), actual.getDouble("BINARY_DOUBLE_COL"));

      CustomAssertions.assertObjectEquals(expected.get("CHAR_COL"), actual.getString("CHAR_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("NCHAR_COL"), actual.getString("NCHAR_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("CHARACTER_COL"), actual.getString("CHARACTER_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("VARCHAR_COL"), actual.getString("VARCHAR_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("VARCHAR2_COL"), actual.getString("VARCHAR2_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("NVARCHAR2_COL"), actual.getString("NVARCHAR2_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("CLOB_COL"), actual.getString("CLOB_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("NCLOB_COL"), actual.getString("NCLOB_COL").trim());
      CustomAssertions.assertObjectEquals(expected.get("ROWID_COL"), actual.getString("ROWID_COL"));
      CustomAssertions.assertObjectEquals(expected.get("UROWID_COL"), actual.getString("UROWID_COL"));

      Assert.assertArrayEquals(expected.get("RAW_COL"), actual.getBytes("RAW_COL"));
      Assert.assertArrayEquals(expected.get("BLOB_COL"), actual.getBytes("BLOB_COL"));

      CustomAssertions.assertObjectEquals("23 3:2:10.0", actual.getString("INTERVAL_DAY_TO_SECOND_COL"));
      CustomAssertions.assertObjectEquals(expected.get("INTERVAL_YEAR_TO_MONTH_COL"),
                                          actual.getString("INTERVAL_YEAR_TO_MONTH_COL"));

      CustomAssertions.assertObjectEquals(expected.getTimestamp("DATE_COL").toEpochSecond(),
                         actual.getTimestamp("DATE_COL").toInstant().getEpochSecond());
      CustomAssertions.assertObjectEquals(expected.getTimestamp("TIMESTAMP_COL").toEpochSecond(),
                         actual.getTimestamp("TIMESTAMP_COL").toInstant().getEpochSecond());
      CustomAssertions.assertObjectEquals(expected.get("TIMESTAMPTZ_COL"), actual.getString("TIMESTAMPTZ_COL"));
      CustomAssertions.assertObjectEquals(expected.getTimestamp("TIMESTAMPLTZ_COL").toEpochSecond(),
                         actual.getTimestamp("TIMESTAMPLTZ_COL").toInstant().getEpochSecond());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  };

  private static final BiConsumer<StructuredRecord, ResultSet> COMPARE_DECIMALS = (expected, actual) -> {
    try {
      CustomAssertions.assertObjectEquals(expected.getDecimal("ID"), actual.getBigDecimal("ID"));
      CustomAssertions.assertObjectEquals(expected.getDecimal("INT_COL"), actual.getBigDecimal("INT_COL"));
      CustomAssertions.assertObjectEquals(expected.getDecimal("INTEGER_COL"), actual.getBigDecimal("INTEGER_COL"));
      CustomAssertions.assertObjectEquals(expected.getDecimal("DEC_COL"), actual.getBigDecimal("DEC_COL"));
      CustomAssertions.assertObjectEquals(expected.getDecimal("DECIMAL_COL"),
                                          actual.getBigDecimal("DECIMAL_COL", SCALE));
      CustomAssertions.assertObjectEquals(expected.getDecimal("NUMBER_COL"),
                                          actual.getBigDecimal("NUMBER_COL", SCALE));
      CustomAssertions.assertObjectEquals(expected.getDecimal("NUMERIC_COL"),
                                          actual.getBigDecimal("NUMERIC_COL", SCALE));
      CustomAssertions.assertObjectEquals(expected.getDecimal("SMALLINT_COL"), actual.getBigDecimal("SMALLINT_COL"));
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  };

  private static final BiConsumer<StructuredRecord, ResultSet> COMPARE_PRIMITIVES = (expected, actual) -> {
    try {
      CustomAssertions.assertObjectEquals(expected.get("ID"), actual.getInt("ID"));
      CustomAssertions.assertObjectEquals(expected.get("INT_COL"), actual.getInt("INT_COL"));
      CustomAssertions.assertObjectEquals(expected.get("INTEGER_COL"), actual.getInt("INTEGER_COL"));
      CustomAssertions.assertObjectEquals(expected.get("DEC_COL"), actual.getLong("DEC_COL"));
      CustomAssertions.assertObjectEquals(expected.get("SMALLINT_COL"), actual.getLong("SMALLINT_COL"));
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  };

  private static final BiConsumer<StructuredRecord, ResultSet> COMPARE_LONG_COLUMN = (expected, actual) -> {
    try {
      // Verify data
      CustomAssertions.assertObjectEquals(expected.get("LONG_COL"), actual.getString("LONG_COL").trim());
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  };

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
    String stringColumnName = "VARCHAR_COL";
    startPipelineAndWriteInvalidData(stringColumnName, getSinkConfig(), DATAPIPELINE_ARTIFACT);
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE")) {
      testInvalidDataWrite(resultSet, stringColumnName);
    }
  }

  @Test
  public void testDBSinkWithExplicitInputSchema() throws Exception {
    testSink("testDBSinkWithExplicitInputSchema", "explicit", MY_DEST_TABLE, DECIMAL_SCHEMA,
             createInputData(), getSinkConfig(), Arrays.asList(COMPARE_DECIMALS, COMPARE_COMMON));
  }

  @Test
  public void testDBSinkWithInferredInputSchema() throws Exception {
    testSink("testDBSinkWithInferredInputSchema", "inferred", MY_DEST_TABLE, null,
             createInputData(), getSinkConfig(), Arrays.asList(COMPARE_DECIMALS, COMPARE_COMMON));
  }

  @Test
  public void testDBSinkPrimitiveToDecimalWithExplicitSchema() throws Exception {
    testSink("testDBSinkPrimitiveToDecimalWithExplicitSchema", "primitive-explicit", MY_DEST_TABLE,
             PRIMITIVE_SCHEMA, createPrimitivesInputData(), getSinkConfig(),
             Arrays.asList(COMPARE_PRIMITIVES, COMPARE_COMMON));
  }

  @Test
  public void testDBSinkPrimitiveToDecimalInferredInputSchema() throws Exception {
    testSink("testDBSinkPrimitiveToDecimalInferredInputSchema", "primitive-inferred", MY_DEST_TABLE,
             null, createPrimitivesInputData(), getSinkConfig(), Arrays.asList(COMPARE_PRIMITIVES, COMPARE_COMMON));
  }

  @Test
  public void testDBSinkLongColumn() throws Exception {
    ETLPlugin sinkConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, MY_DEST_TABLE_FOR_LONG)
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);

    testSink("testDBSinkLongColumn", "long-column", MY_DEST_TABLE_FOR_LONG, null,
             createLongColumnInputData(), sinkConfig, Collections.singletonList(COMPARE_LONG_COLUMN));
  }

  @Test
  public void testDBSinkSingleFieldInferredInputSchema() throws Exception {
    Schema schema = Schema.recordOf("dbRecord", Schema.Field.of("ID", Schema.of(Schema.Type.INT)));
    StructuredRecord record = StructuredRecord.builder(schema).set("ID", 1).build();
    BiConsumer<StructuredRecord, ResultSet> test = (expected, actual) -> {
      try {
        CustomAssertions.assertObjectEquals(actual.getInt("ID"), 1);
      } catch (SQLException e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    };

    testSink("testDBSinkSingleFieldInferredInputSchema", "single-field-inferred",
             MY_DEST_TABLE, null, Collections.singletonList(record), getSinkConfig(), Collections.singletonList(test));
  }

  @Test
  public void testDBSinkSingleFieldExplicitInputSchema() throws Exception {
    Schema schema = Schema.recordOf("dbRecord", Schema.Field.of("ID", Schema.of(Schema.Type.INT)));
    StructuredRecord record = StructuredRecord.builder(schema).set("ID", 1).build();
    BiConsumer<StructuredRecord, ResultSet> test = (expected, actual) -> {
      try {
        CustomAssertions.assertObjectEquals(actual.getInt("ID"), 1);
      } catch (SQLException e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    };

    testSink("testDBSinkSingleFieldExplicitInputSchema", "single-field-explicit", MY_DEST_TABLE,
             schema, Collections.singletonList(record), getSinkConfig(), Collections.singletonList(test));
  }

  private void testSink(String appName,
                        String inputDatasetName,
                        String tableName,
                        Schema schema,
                        List<StructuredRecord> inputRecords,
                        ETLPlugin sinkConfig,
                        List<BiConsumer<StructuredRecord, ResultSet>> testActions) throws Exception {

    ETLPlugin sourceConfig = (schema != null)
      ? MockSource.getPlugin(inputDatasetName, schema)
      : MockSource.getPlugin(inputDatasetName);

    ApplicationManager applicationManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, appName);
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, inputRecords);
    runETLOnce(applicationManager);

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet actual = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY ID")) {

      for (StructuredRecord expected : inputRecords) {
        Assert.assertTrue(actual.next());
        // Perform supplied test actions
        testActions.forEach(test -> test.accept(expected, actual));
      }
    }
  }

  @Override
  protected void writeDataForInvalidDataWriteTest(String inputDatasetName, String stringColumnName) throws Exception {
    Schema validSchema = Schema.recordOf(
      "validDBRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of(stringColumnName, Schema.of(Schema.Type.STRING))
    );

    Schema invalidSchema = Schema.recordOf(
      "wrongDBRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of(stringColumnName, Schema.of(Schema.Type.INT))
    );

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);

    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(validSchema)
                       .set("ID", "1".getBytes())
                       .set(stringColumnName, "user1")
                       .build());
    inputRecords.add(StructuredRecord.builder(invalidSchema)
                       .set("ID", "2".getBytes())
                       .set(stringColumnName, 1)
                       .build());
    inputRecords.add(StructuredRecord.builder(validSchema)
                       .set("ID", "3".getBytes())
                       .set(stringColumnName, "user3")
                       .build());
    MockSource.writeInput(inputManager, inputRecords);
  }

  private List<StructuredRecord> createInputData() throws Exception {
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      StructuredRecord.Builder builder = StructuredRecord.builder(DECIMAL_SCHEMA)
        .setDecimal("ID", new BigDecimal(i, new MathContext(DEFAULT_PRECISION)).setScale(0))
        .set("CHAR_COL", name)
        .set("NCHAR_COL", name)
        .set("CHARACTER_COL", name)
        .set("VARCHAR_COL", name)
        .set("VARCHAR2_COL", name)
        .set("NVARCHAR2_COL", name)
        .set("CLOB_COL", name)
        .set("NCLOB_COL", name)
        .setDecimal("INT_COL", new BigDecimal(31 + i, new MathContext(DEFAULT_PRECISION)).setScale(0))
        .setDecimal("INTEGER_COL", new BigDecimal(42 + i, new MathContext(DEFAULT_PRECISION)).setScale(0))
        .setDecimal("DEC_COL", new BigDecimal(24 + i, new MathContext(PRECISION)).setScale(0))
        .setDecimal("DECIMAL_COL", new BigDecimal(3.456, new MathContext(PRECISION)).setScale(SCALE))
        .setDecimal("NUMBER_COL", new BigDecimal(3.456, new MathContext(PRECISION)).setScale(SCALE))
        .setDecimal("NUMERIC_COL", new BigDecimal(3.457, new MathContext(PRECISION)).setScale(SCALE))
        // It's safe to store such values using SMALLINT since it's actually NUMBER(38, 0)
        .setDecimal("SMALLINT_COL", new BigDecimal(Long.MAX_VALUE, new MathContext(DEFAULT_PRECISION))
          .setScale(0)
          .add(new BigDecimal(Long.MAX_VALUE)))
        .setTimestamp("DATE_COL", localDateTime.atZone(UTC))
        .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(UTC))
        .set("INTERVAL_YEAR_TO_MONTH_COL", "300-5")
        .set("INTERVAL_DAY_TO_SECOND_COL", "23 3:02:10")
        .set("RAW_COL", name.getBytes())
        .set("BLOB_COL", name.getBytes())
        .set("FLOAT_COL", 3.14d)
        .set("REAL_COL", 3.14d)
        .set("LONG_RAW_COL", name.getBytes())
        .set("BINARY_FLOAT_COL", 3.14f)
        .set("BINARY_DOUBLE_COL", 3.14d)
        .set("ROWID_COL", "AAAUEVAAFAAAAR/AA" + i)
        .set("UROWID_COL", "AAAUEVAAFAAAAR/AA" + i)
        .set("TIMESTAMPTZ_COL", "2019-07-15 15:57:46.65 GMT")
        .setTimestamp("TIMESTAMPLTZ_COL", localDateTime.atZone(UTC));

      inputRecords.add(builder.build());
    }

    return inputRecords;
  }

  private List<StructuredRecord> createPrimitivesInputData() throws Exception {
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      StructuredRecord.Builder builder = StructuredRecord.builder(PRIMITIVE_SCHEMA)
        .set("ID", i)
        .set("CHAR_COL", name)
        .set("NCHAR_COL", name)
        .set("CHARACTER_COL", name)
        .set("VARCHAR_COL", name)
        .set("VARCHAR2_COL", name)
        .set("NVARCHAR2_COL", name)
        .set("CLOB_COL", name)
        .set("NCLOB_COL", name)
        .set("INT_COL", 31 + i)
        .set("INTEGER_COL", 42 + i)
        .set("DEC_COL", 24L + i)
        .set("SMALLINT_COL", Long.MAX_VALUE) // SMALLINT is actually NUMBER(38, 0)
        .setTimestamp("DATE_COL", localDateTime.atZone(UTC))
        .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(UTC))
        .set("INTERVAL_YEAR_TO_MONTH_COL", "300-5")
        .set("INTERVAL_DAY_TO_SECOND_COL", "23 3:02:10")
        .set("RAW_COL", name.getBytes())
        .set("BLOB_COL", name.getBytes())
        .set("FLOAT_COL", 3.14d)
        .set("REAL_COL", 3.14d)
        .set("LONG_RAW_COL", name.getBytes())
        .set("BINARY_FLOAT_COL", 3.14f)
        .set("BINARY_DOUBLE_COL", 3.14d)
        .set("ROWID_COL", "AAAUEVAAFAAAAR/AA" + i)
        .set("UROWID_COL", "AAAUEVAAFAAAAR/AA" + i)
        .set("TIMESTAMPTZ_COL", "2019-07-15 15:57:46.65 GMT")
        .setTimestamp("TIMESTAMPLTZ_COL", localDateTime.atZone(UTC));

      inputRecords.add(builder.build());
    }

    return inputRecords;
  }

  private List<StructuredRecord> createLongColumnInputData() throws Exception {
    List<StructuredRecord> inputRecords = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      StructuredRecord.Builder builder = StructuredRecord.builder(LONG_COLUMN_SCHEMA)
        .set("ID", i)
        .set("SMALLINT_COL", Long.MAX_VALUE) // SMALLINT is actually NUMBER(38, 0)
        .set("VARCHAR_COL", name)
        .set("LONG_COL", name);

      inputRecords.add(builder.build());
    }

    return inputRecords;
  }

  private ETLPlugin getSinkConfig() {
   return new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);
  }
}

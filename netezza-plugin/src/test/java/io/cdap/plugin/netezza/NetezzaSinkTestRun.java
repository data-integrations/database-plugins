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

package io.cdap.plugin.netezza;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test for ETL using databases.
 */
public class NetezzaSinkTestRun extends NetezzaPluginTestBase {
  private static final Schema SCHEMA = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("INTEGER_COL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("BYTEINT_COL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("SMALLINT_COL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("BIGINT_COL", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("REAL_COL", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("REAL_FLOAT_COL", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("DOUBLE_FLOAT_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("DOUBLE_PRECISION_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("NUMERIC_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("VARCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("NVARCHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("VARBINARY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("ST_GEOMETRY_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("TIMETZ_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("INTERVAL_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("BOOLEAN_COL", Schema.of(Schema.Type.BOOLEAN))
  );

  private static final BigDecimal NUMERIC_VALUE = new BigDecimal(3.458d, new MathContext(PRECISION)).setScale(SCALE);

  @Before
  public void setup() throws Exception {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute("TRUNCATE TABLE MY_DEST_TABLE");
    }
  }

  @Test
  public void testDBSinkWithInvalidFieldType() throws Exception {
    testDBInvalidFieldType("INTEGER_COL", Schema.Type.STRING, getSinkConfig(), DATAPIPELINE_ARTIFACT);
  }

  @Test
  public void testDBSinkWithInvalidFieldLogicalType() throws Exception {
    testDBInvalidFieldLogicalType("TIMESTAMP_COL", Schema.Type.LONG, getSinkConfig(), DATAPIPELINE_ARTIFACT);
  }

  @Test
  public void testDBSinkWithDBSchemaAndInvalidData() throws Exception {
    // Netezza JDBC connector allows to write integer values to STRING column. Use BOOLEAN column instead.
    String booleanColumnName = "BOOLEAN_COL";
    startPipelineAndWriteInvalidData(booleanColumnName, getSinkConfig(), DATAPIPELINE_ARTIFACT);
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE")) {
      testInvalidDataWrite(resultSet, booleanColumnName);
    }
  }

  @Test
  public void testDBSinkWithExplicitInputSchema() throws Exception {
    testDBSink("testDBSinkWithExplicitInputSchema", "input-dbsinktest-explicit", true);
  }


  @Test
  public void testDBSinkWithInferredInputSchema() throws Exception {
    testDBSink("testDBSinkWithInferredInputSchema", "input-dbsinktest-inferred", false);
  }

  public void testDBSink(String appName, String inputDatasetName, boolean setInputSchema) throws Exception {
    ETLPlugin sourceConfig = (setInputSchema)
      ? MockSource.getPlugin(inputDatasetName, SCHEMA)
      : MockSource.getPlugin(inputDatasetName);
    ETLPlugin sinkConfig = getSinkConfig();

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, appName);
    createInputData(inputDatasetName);

    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE ORDER BY INTEGER_COL")) {

      Set<String> users = new HashSet<>();
      Assert.assertTrue(resultSet.next());
      users.add(resultSet.getString("VARCHAR_COL"));

      Assert.assertEquals(1, resultSet.getInt("INTEGER_COL"));
      Assert.assertEquals(1, resultSet.getInt("BYTEINT_COL"));
      Assert.assertEquals(1, resultSet.getInt("SMALLINT_COL"));
      Assert.assertEquals(1, resultSet.getLong("BIGINT_COL"));

      Assert.assertEquals(3.451f + 1, resultSet.getFloat("REAL_COL"), 0.00001f);
      Assert.assertEquals(3.451f + 1, resultSet.getFloat("REAL_FLOAT_COL"), 0.00001f);
      Assert.assertEquals(3.451 + 1, resultSet.getFloat("DOUBLE_FLOAT_COL"), 0.000001);
      Assert.assertEquals(3.451 + 1, resultSet.getFloat("DOUBLE_PRECISION_COL"), 0.000001);
      Assert.assertEquals(NUMERIC_VALUE, resultSet.getBigDecimal("NUMERIC_COL"));

      Assert.assertEquals("user1", resultSet.getString("CHAR_COL").trim());
      Assert.assertEquals("user1", resultSet.getString("VARCHAR_COL"));
      Assert.assertEquals("user1", resultSet.getString("NCHAR_COL").trim());
      Assert.assertEquals("user1", resultSet.getString("NVARCHAR_COL"));
      Assert.assertEquals("user1", Bytes.toString(resultSet.getBytes("VARBINARY_COL")));
      Assert.assertEquals("user1", Bytes.toString(resultSet.getBytes("ST_GEOMETRY_COL")));

      Assert.assertEquals(new Date(CURRENT_TS).toString(), resultSet.getDate("DATE_COL").toString());
      Assert.assertEquals(new Time(CURRENT_TS).toString(), resultSet.getTime("TIME_COL").toString());
      Assert.assertEquals("13:24:16+03", resultSet.getString("TIMETZ_COL"));
      Assert.assertEquals(new Timestamp(CURRENT_TS), resultSet.getTimestamp("TIMESTAMP_COL"));
      Assert.assertEquals("2 years 3 mons 2 days", resultSet.getString("INTERVAL_COL"));

      Assert.assertTrue(resultSet.getBoolean("BOOLEAN_COL"));

      Assert.assertTrue(resultSet.next());
      users.add(resultSet.getString("VARCHAR_COL"));

      Assert.assertEquals(ImmutableSet.of("user1", "user2"), users);
    }
  }

  private void createInputData(String inputDatasetName) throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      inputRecords.add(StructuredRecord.builder(SCHEMA)
                         .set("INTEGER_COL", i)
                         .set("BYTEINT_COL", i)
                         .set("SMALLINT_COL", i)
                         .set("BIGINT_COL", (long) i)
                         .set("REAL_COL", 3.451f + i)
                         .set("REAL_FLOAT_COL", 3.451f + i)
                         .set("DOUBLE_FLOAT_COL", 3.451 + i)
                         .set("DOUBLE_PRECISION_COL", 3.451 + i)
                         .setDecimal("NUMERIC_COL", NUMERIC_VALUE)
                         .setDecimal("DECIMAL_COL", NUMERIC_VALUE)
                         .set("CHAR_COL", name)
                         .set("VARCHAR_COL", name)
                         .set("NCHAR_COL", name)
                         .set("NVARCHAR_COL", name)
                         .set("VARBINARY_COL", name.getBytes(Charsets.UTF_8))
                         .set("ST_GEOMETRY_COL", name.getBytes(Charsets.UTF_8))
                         .setDate("DATE_COL", localDateTime.toLocalDate())
                         .setTime("TIME_COL", localDateTime.toLocalTime())
                         .set("TIMETZ_COL", "13:24:16+03")
                         .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
                         .set("INTERVAL_COL", "2 years 3 mons 2 days")
                         .set("BOOLEAN_COL", true)
                         .build());
    }
    MockSource.writeInput(inputManager, inputRecords);
  }

  private ETLPlugin getSinkConfig() {
    return new ETLPlugin(
      NetezzaConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);
  }
}

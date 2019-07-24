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

package io.cdap.plugin.postgres;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
public class PostgresSinkTestRun extends PostgresPluginTestBase {
  private static final Schema SCHEMA = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
    Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("SCORE", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("GRADUATED", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("NOT_IMPORTED", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("SMALLINT_COL", Schema.of(Schema.Type.INT)),
    Schema.Field.of("BIG", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("NUMERIC_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)),
    Schema.Field.of("DOUBLE_PREC_COL", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("TEXT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("BYTEA_COL", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("BIT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("VAR_BIT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TIMETZ_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TIMESTAMPTZ_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("XML_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("UUID_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("CIDR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("CIRCLE_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INET_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("INTERVAL_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("JSON_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("JSONB_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("LINE_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("LSEG_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("MACADDR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("MACADDR8_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("MONEY_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("PATH_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("POINT_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("POLYGON_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TSQUERY_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TSVECTOR_COL", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("BOX_COL", Schema.of(Schema.Type.STRING))
  );

  @Before
  public void setup() throws Exception {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute("TRUNCATE TABLE \"MY_DEST_TABLE\"");
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
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM \"MY_DEST_TABLE\"")) {
      testInvalidDataWrite(resultSet, stringColumnName);
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
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM \"MY_DEST_TABLE\" ORDER BY \"ID\" ASC")) {
      Set<String> users = new HashSet<>();
      Assert.assertTrue(resultSet.next());
      users.add(resultSet.getString("NAME"));
      Assert.assertEquals(new Date(CURRENT_TS).toString(), resultSet.getDate("DATE_COL").toString());
      Assert.assertEquals(new Time(CURRENT_TS).toString(), resultSet.getTime("TIME_COL").toString());
      Assert.assertEquals(new Timestamp(CURRENT_TS),
                          resultSet.getTimestamp("TIMESTAMP_COL"));
      Assert.assertEquals(new Timestamp(CURRENT_TS),
                          resultSet.getTimestamp("TIMESTAMPTZ_COL"));
      Assert.assertTrue(resultSet.next());
      Assert.assertArrayEquals("user2".getBytes(), resultSet.getBytes("BYTEA_COL"));
      Assert.assertEquals(new BigDecimal(3.458, new MathContext(PRECISION)).setScale(SCALE),
                          resultSet.getBigDecimal("NUMERIC_COL"));
      Assert.assertEquals(new BigDecimal(3.459, new MathContext(PRECISION)).setScale(SCALE),
                          resultSet.getBigDecimal("DECIMAL_COL"));

      users.add(resultSet.getString("NAME"));
      Assert.assertEquals(ImmutableSet.of("user1", "user2"), users);

      Assert.assertEquals(3.451f, resultSet.getFloat("SCORE"), 0.00001);
      Assert.assertTrue(resultSet.getBoolean("GRADUATED"));
      Assert.assertEquals(4, resultSet.getInt("SMALLINT_COL"));
      Assert.assertEquals(3456987L, resultSet.getLong("BIG"));
      Assert.assertEquals(3.459d, resultSet.getFloat("DOUBLE_PREC_COL"), 0.00001);
      Assert.assertEquals("user2", resultSet.getString("TEXT_COL"));
      Assert.assertEquals("char2", resultSet.getString("CHAR_COL").trim());
      Assert.assertEquals("1010", resultSet.getString("BIT_COL"));
      Assert.assertEquals("101", resultSet.getString("VAR_BIT_COL"));
      Assert.assertEquals("03:02:03.456+03", resultSet.getString("TIMETZ_COL"));
      Assert.assertEquals("<root></root>", resultSet.getString("XML_COL"));
      Assert.assertEquals("e95861c9-1111-40ce-b42b-d6b9d1765c2c", resultSet.getString("UUID_COL"));
      Assert.assertEquals("192.168.0.0/23", resultSet.getString("CIDR_COL"));
      Assert.assertEquals("<(1,2),10>", resultSet.getString("CIRCLE_COL"));
      Assert.assertEquals("192.168.1.1", resultSet.getString("INET_COL"));
      Assert.assertEquals("1 day", resultSet.getString("INTERVAL_COL"));
      Assert.assertEquals("{\"hello\": \"world\"}", resultSet.getString("JSON_COL"));
      Assert.assertEquals("{\"hello\": \"world\"}", resultSet.getString("JSONB_COL"));
      Assert.assertEquals("{1,-1,0}", resultSet.getString("LINE_COL"));
      Assert.assertEquals("[(1,1),(2,2)]", resultSet.getString("LSEG_COL"));
      Assert.assertEquals("08:00:2b:01:02:03", resultSet.getString("MACADDR_COL"));
      Assert.assertEquals("08:00:2b:01:02:03:04:05", resultSet.getString("MACADDR8_COL"));
      Assert.assertEquals("$1,234.12", resultSet.getString("MONEY_COL"));
      Assert.assertEquals("[(1,1),(2,2),(3,3)]", resultSet.getString("PATH_COL"));
      Assert.assertEquals("(1,1)", resultSet.getString("POINT_COL"));
      Assert.assertEquals("((1,1),(2,2),(0,5))", resultSet.getString("POLYGON_COL"));
      Assert.assertEquals("'fat' & ( 'rat' | 'cat' )", resultSet.getString("TSQUERY_COL"));
      Assert.assertEquals("'a' 'cat' 'fat'", resultSet.getString("TSVECTOR_COL"));
      Assert.assertEquals("(2,2),(1,1)", resultSet.getString("BOX_COL"));
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
                         .set("ID", i)
                         .set("NAME", name)
                         .set("SCORE", 3.451f)
                         .set("GRADUATED", (i % 2 == 0))
                         .set("NOT_IMPORTED", "random" + i)
                         .set("SMALLINT_COL", i + 2)
                         .set("BIG", 3456987L)
                         .setDecimal("NUMERIC_COL", new BigDecimal(3.458d, new MathContext(PRECISION)).setScale(SCALE))
                         .setDecimal("DECIMAL_COL", new BigDecimal(3.459d, new MathContext(PRECISION)).setScale(SCALE))
                         .set("DOUBLE_PREC_COL", 3.459d)
                         .setDate("DATE_COL", localDateTime.toLocalDate())
                         .setTime("TIME_COL", localDateTime.toLocalTime())
                         .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
                         .set("TEXT_COL", name)
                         .set("CHAR_COL", "char" + i)
                         .set("BYTEA_COL", name.getBytes(Charsets.UTF_8))
                         .set("BIT_COL", "1010")
                         .set("VAR_BIT_COL", "101")
                         .set("TIMETZ_COL", "03:02:03.456+03")
                         .setTimestamp("TIMESTAMPTZ_COL", localDateTime.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
                         .set("XML_COL", "<root></root>")
                         .set("UUID_COL", "e95861c9-1111-40ce-b42b-d6b9d1765c2c")
                         .set("CIDR_COL", "192.168.0.0/23")
                         .set("CIRCLE_COL", "<(1.0,2.0),10.0>")
                         .set("INET_COL", "192.168.1.1")
                         .set("INTERVAL_COL", "1 day")
                         .set("JSON_COL", "{\"hello\": \"world\"}")
                         .set("JSONB_COL", "{\"hello\": \"world\"}")
                         .set("LINE_COL", "((1.0, 1.0),(2.0, 2.0))")
                         .set("LSEG_COL", "[(1,1),(2,2)]")
                         .set("MACADDR_COL", "08:00:2b:01:02:03")
                         .set("MACADDR8_COL", "08:00:2b:01:02:03:04:05")
                         .set("MONEY_COL", "1234.12")
                         .set("PATH_COL", "[(1.0, 1.0),(2.0, 2.0), (3.0, 3.0)]")
                         .set("POINT_COL" , "(1.0, 1.0)")
                         .set("POLYGON_COL", "((1.0, 1.0),(2.0, 2.0), (0.0, 5.0))")
                         .set("TSQUERY_COL", "'fat' & ( 'rat' | 'cat' )")
                         .set("TSVECTOR_COL", "'a' 'cat' 'fat'")
                         .set("BOX_COL", "(2,2),(1,1)")
                         .build());
    }
    MockSource.writeInput(inputManager, inputRecords);
  }

  private ETLPlugin getSinkConfig() {
    return new ETLPlugin(
      PostgresConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);
  }
}

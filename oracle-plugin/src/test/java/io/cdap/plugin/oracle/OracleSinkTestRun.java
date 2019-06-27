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
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test for ETL using databases.
 */
public class OracleSinkTestRun extends OraclePluginTestBase {

  @Test
  public void testDBSink() throws Exception {

    String inputDatasetName = "input-dbsinktest";

    ETLPlugin sourceConfig = MockSource.getPlugin(inputDatasetName);
    ETLPlugin sinkConfig = new ETLPlugin(
      OracleConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);

    deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, "testDBSink");
    createInputData(inputDatasetName);


    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM my_table")) {
      Set<String> users = new HashSet<>();
      Assert.assertTrue(resultSet.next());
      users.add(resultSet.getString("VARCHAR_COL"));
      Assert.assertEquals(new Date(CURRENT_TS).toString(), resultSet.getDate("DATE_COL").toString());
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals("user2", resultSet.getString("CLOB_COL"));
      Assert.assertEquals("user2", resultSet.getString("NCLOB_COL"));
      Assert.assertEquals("user2", Bytes.toString(resultSet.getBytes("BLOB_COL"), 0, 5));
      Assert.assertEquals(new BigDecimal(3.456, new MathContext(PRECISION)).setScale(SCALE),
                          resultSet.getBigDecimal("DECIMAL_COL"));
      Assert.assertEquals(new BigDecimal(3.456, new MathContext(PRECISION)).setScale(SCALE),
                          resultSet.getBigDecimal("NUMBER_COL"));
      Assert.assertEquals(new BigDecimal(3.457, new MathContext(PRECISION)).setScale(SCALE),
                          resultSet.getBigDecimal("NUMERIC_COL"));
      users.add(resultSet.getString("VARCHAR_COL"));
      Assert.assertEquals(ImmutableSet.of("user1", "user2"), users);

    }
  }

  private void createInputData(String inputDatasetName) throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Schema schema = Schema.recordOf(
      "dbRecord",
      Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("CHARACTER_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("VARCHAR_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("VARCHAR2_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("INT_COL", Schema.of(Schema.Type.INT)),
      Schema.Field.of("INTEGER_COL", Schema.of(Schema.Type.INT)),
      Schema.Field.of("DEC_COL", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)),
      Schema.Field.of("NUMBER_COL", Schema.decimalOf(PRECISION, SCALE)),
      Schema.Field.of("NUMERIC_COL", Schema.decimalOf(PRECISION, SCALE)),
      Schema.Field.of("SMALLINT_COL", Schema.of(Schema.Type.INT)),
      Schema.Field.of("REAL_COL", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("INTERVAL_YEAR_TO_MONTH_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("INTERVAL_DAY_TO_SECOND_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("RAW_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("CLOB_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("NCLOB_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("BLOB_COL", Schema.of(Schema.Type.BYTES))
    );
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      inputRecords.add(StructuredRecord.builder(schema)
                         .set("CHAR_COL", name)
                         .set("CHARACTER_COL", name)
                         .set("VARCHAR_COL", name)
                         .set("VARCHAR2_COL", name)
                         .set("INT_COL", 31 + i)
                         .set("INTEGER_COL", 42 + i)
                         .set("DEC_COL", (double) 24 + i)
                         .setDecimal("DECIMAL_COL", new BigDecimal(3.456, new MathContext(PRECISION)).setScale(SCALE))
                         .setDecimal("NUMBER_COL", new BigDecimal(3.456, new MathContext(PRECISION)).setScale(SCALE))
                         .setDecimal("NUMERIC_COL", new BigDecimal(3.457, new MathContext(PRECISION)).setScale(SCALE))
                         .set("SMALLINT_COL", 1)
                         .set("REAL_COL", 3.14f)
                         .setDate("DATE_COL", localDateTime.toLocalDate())
                         .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(ZoneId.systemDefault()))
                         .set("INTERVAL_YEAR_TO_MONTH_COL", "300-5")
                         .set("INTERVAL_DAY_TO_SECOND_COL", "23 3:02:10")
                         .set("RAW_COL", name.getBytes())
                         .set("CLOB_COL", name.getBytes())
                         .set("NCLOB_COL", name.getBytes())
                         .set("BLOB_COL", name.getBytes())
                         .build());
    }
    MockSource.writeInput(inputManager, inputRecords);
  }
}

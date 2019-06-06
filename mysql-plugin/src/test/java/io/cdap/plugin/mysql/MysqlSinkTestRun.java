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
import org.junit.Test;

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
public class MysqlSinkTestRun extends MysqlPluginTestBase {

  @Test
  public void testDBSink() throws Exception {
    String inputDatasetName = "input-dbsinktest";

    ETLPlugin sourceConfig = MockSource.getPlugin(inputDatasetName);
    ETLPlugin sinkConfig = new ETLPlugin(
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

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, "testDBSink");
    createInputData(inputDatasetName);
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE")) {
      Set<String> users = new HashSet<>();
      Assert.assertTrue(resultSet.next());
      users.add(resultSet.getString("NAME"));
      Assert.assertEquals(new Date(CURRENT_TS).toString(), resultSet.getDate("DATE_COL").toString());
      Assert.assertEquals(new Time(CURRENT_TS).toString(), resultSet.getTime("TIME_COL").toString());
      Assert.assertEquals(new Timestamp(CURRENT_TS),
                          resultSet.getTimestamp("TIMESTAMP_COL"));
      Assert.assertTrue(resultSet.next());
      users.add(resultSet.getString("NAME"));
      Assert.assertEquals(ImmutableSet.of("user1", "user2"), users);

    }
  }

  private void createInputData(String inputDatasetName) throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Schema schema = Schema.recordOf(
      "dbRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("SCORE", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("GRADUATED", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("TINY", Schema.of(Schema.Type.INT)),
      Schema.Field.of("SMALL", Schema.of(Schema.Type.INT)),
      Schema.Field.of("BIG", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("FLOAT_COL", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("REAL_COL", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("NUMERIC_COL", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("DECIMAL_COL", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("BIT_COL", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("BINARY_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("BLOB_COL", Schema.of(Schema.Type.BYTES))
    );
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      inputRecords.add(StructuredRecord.builder(schema)
                         .set("ID", i)
                         .set("NAME", name)
                         .set("SCORE", 3.451)
                         .set("GRADUATED", (i % 2 == 0))
                         .set("TINY", i + 1)
                         .set("SMALL", i + 2)
                         .set("BIG", 3456987L)
                         .set("FLOAT_COL", 3.456f)
                         .set("REAL_COL", 3.457)
                         .set("NUMERIC_COL", 3.458d)
                         .set("DECIMAL_COL", 3.459d)
                         .set("BIT_COL", (i % 2 == 1))
                         .setDate("DATE_COL", localDateTime.toLocalDate())
                         .setTime("TIME_COL", localDateTime.toLocalTime())
                         .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
                         .set("BINARY_COL", name.getBytes(Charsets.UTF_8))
                         .set("BLOB_COL", name.getBytes(Charsets.UTF_8))
                         .build());
    }
    MockSource.writeInput(inputManager, inputRecords);
  }
}

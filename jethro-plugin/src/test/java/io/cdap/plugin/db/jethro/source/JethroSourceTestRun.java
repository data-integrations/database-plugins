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

package io.cdap.plugin.db.jethro.source;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.db.jethro.JethroPluginTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JethroSourceTestRun extends JethroPluginTestBase {

  @Test
  public void testCheckAllTypes() throws Exception {
    String importQuery =
      "SELECT * FROM test_table";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(Constants.Reference.REFERENCE_NAME, "jethroSource")
      .putAll(BASE_PROPS)
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "1")
      .build();

    ETLPlugin sourceConfig = new ETLPlugin(
      "JethroSource",
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("sinkOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testAllTypes");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("sinkOutputTable");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(5, records.size());

    java.util.Date date = new java.util.Date(CURRENT_TS);
    ZonedDateTime expectedTs = date.toInstant().atZone(UTC_ZONE);
    AtomicInteger i = new AtomicInteger();
    records.forEach(record -> {
      Assert.assertEquals(1 + i.get(), (int) record.get("int_value"));
      Assert.assertEquals(100 + i.get(), (long) record.get("long_value"));
      Assert.assertEquals(0.1 + i.get(), (float) record.get("float_value"), 0.01);
      Assert.assertEquals(0.03 + i.get(), (double) record.get("double_value"), 0.01);
      if (i.get() != 4) {
        Assert.assertEquals("Test_" + i, record.get("string_value"));
      } else {
        Assert.assertNull(record.get("timestamp_value"));
        Assert.assertNull(record.get("string_value"));
      }
      i.getAndIncrement();
    });
  }

  @Test
  public void testCheckBoundingQuery() throws Exception {
    String importQuery =
      "SELECT * FROM test_table WHERE int_value = 3 AND $CONDITIONS";
    String conditions = "SELECT MIN(int_value), MAX(int_value) FROM test_table";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(Constants.Reference.REFERENCE_NAME, "jethroSource")
      .putAll(BASE_PROPS)
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, conditions)
      .put(AbstractDBSource.DBSourceConfig.NUM_SPLITS, "2")
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, "int_value")
      .build();

    ETLPlugin sourceConfig = new ETLPlugin(
      "JethroSource",
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("sinkBoundingQuery");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testBoundingQuery");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("sinkBoundingQuery");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(1, records.size());

    StructuredRecord record = records.get(0);
    Assert.assertEquals(3, (int) record.get("int_value"));
    Assert.assertEquals(102, (long) record.get("long_value"));
    Assert.assertEquals(2.1, (float) record.get("float_value"), 0.01);
    Assert.assertEquals(2.03, (double) record.get("double_value"), 0.01);
    Assert.assertEquals("Test_2", record.get("string_value"));

  }
}

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

package io.cdap.plugin.neo4j.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.db.CustomAssertions;
import io.cdap.plugin.neo4j.Neo4jConstants;
import io.cdap.plugin.neo4j.NeojlPluginTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jSinkTestRun extends NeojlPluginTestBase {
  private static final Schema DURATION_SCHEMA = Schema.recordOf(
    "duration",
    Schema.Field.of("duration", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("months", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("days", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("seconds", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("nanoseconds", Schema.of(Schema.Type.INT))
  );

  private static final Schema POINT_2D_SCHEMA = Schema.recordOf(
    "point2d",
    Schema.Field.of("crs", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("srid", Schema.of(Schema.Type.INT))
  );

  private static final Schema POINT_3D_SCHEMA = Schema.recordOf(
    "point3d",
    Schema.Field.of("crs", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("z", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("srid", Schema.of(Schema.Type.INT))
  );

  private static final Schema GEO_2D_SCHEMA = Schema.recordOf(
    "geo2d",
    Schema.Field.of("crs", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("latitude", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("srid", Schema.of(Schema.Type.INT)),
    Schema.Field.of("longitude", Schema.of(Schema.Type.DOUBLE))
  );

  private static final Schema GEO_3D_SCHEMA = Schema.recordOf(
    "geo3d",
    Schema.Field.of("crs", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("latitude", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("z", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("srid", Schema.of(Schema.Type.INT)),
    Schema.Field.of("longitude", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("height", Schema.of(Schema.Type.DOUBLE))
  );

  private static final Schema SCHEMA = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("string_val", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("double_val", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("long_val", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("boolean_val", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("array_int", Schema.arrayOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("array_string", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("date_val", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("time_val", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("timestamp_val", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("duration_val", DURATION_SCHEMA),
    Schema.Field.of("point_2d_val", POINT_2D_SCHEMA),
    Schema.Field.of("point_3d_val", POINT_3D_SCHEMA),
    Schema.Field.of("geo_2d_val", GEO_2D_SCHEMA),
    Schema.Field.of("geo_3d_val", GEO_3D_SCHEMA)
  );

  @Test
  public void testStoreAllFieldsAsRecord() throws Exception {
    ETLPlugin sourceConfig = MockSource.getPlugin("Neo4jSource", SCHEMA);

    ETLPlugin sinkConfig = new ETLPlugin(
      "Neo4jSink",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(Neo4jConstants.NAME_REFERENCE_NAME, "neo4jSinkStoreAll")
        .put(Neo4jSinkConfig.NAME_OUTPUT_QUERY, "CREATE (n:CDAP_TEST_1 $(*))")
        .build(),
      null);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT,
                                              "testStoreAllProperties");

    // Prepare test input data
    List<StructuredRecord> inputRecords = createInputData();
    DataSetManager<Table> inputManager = getDataset("Neo4jSource");
    MockSource.writeInput(inputManager, inputRecords);
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet actual = stmt.executeQuery("MATCH (n:CDAP_TEST_1) RETURN n")) {

      for (StructuredRecord expected : inputRecords) {
        Assert.assertTrue(actual.next());
        Map node = (Map) actual.getObject("n");

        CustomAssertions.assertObjectEquals(expected.get("int_val"), node.get("int_val"));
        CustomAssertions.assertObjectEquals(expected.get("string_val"), node.get("string_val"));
        CustomAssertions.assertObjectEquals(expected.get("double_val"), node.get("double_val"));
        CustomAssertions.assertObjectEquals(expected.get("long_val"), node.get("long_val"));
        CustomAssertions.assertObjectEquals(expected.get("boolean_val"), node.get("boolean_val"));
        int[] intArray = expected.get("array_int");
        List intList = (List) node.get("array_int");
        Assert.assertEquals(intArray.length, intList.size());
        for (int i = 0; i < intArray.length; i++) {
          Assert.assertEquals((long) intArray[i], intList.get(i));
        }
        String[] stringArray = expected.get("array_string");
        List stringList = (List) node.get("array_string");
        Assert.assertEquals(stringArray.length, stringList.size());
        for (int i = 0; i < stringArray.length; i++) {
          Assert.assertEquals(stringArray[i], stringList.get(i));
        }
        Assert.assertEquals(expected.getDate("date_val"), ((Date) node.get("date_val")).toLocalDate());
        Assert.assertEquals(expected.getTime("time_val").toSecondOfDay(),
                            ((Time) node.get("time_val")).toLocalTime().toSecondOfDay());
        Assert.assertEquals(expected.getTimestamp("timestamp_val"),
                            ((Timestamp) node.get("timestamp_val")).toInstant().atZone(UTC_ZONE));

        StructuredRecord record = expected.get("duration_val");
        Map properties = (Map) node.get("duration_val");
        Assert.assertEquals(record.get("duration"), properties.get("duration"));
        Assert.assertEquals(record.get("months"), properties.get("months"));
        Assert.assertEquals(record.get("days"), properties.get("days"));
        Assert.assertEquals(record.get("seconds"), properties.get("seconds"));
        Assert.assertEquals(record.get("nanoseconds"), properties.get("nanoseconds"));

        record = expected.get("point_2d_val");
        properties = (Map) node.get("point_2d_val");
        Assert.assertEquals(record.get("crs"), properties.get("crs"));
        Assert.assertEquals(record.get("x"), properties.get("x"));
        Assert.assertEquals(record.get("y"), properties.get("y"));
        Assert.assertEquals(record.get("srid"), properties.get("srid"));

        record = expected.get("point_3d_val");
        properties = (Map) node.get("point_3d_val");
        Assert.assertEquals(record.get("crs"), properties.get("crs"));
        Assert.assertEquals(record.get("x"), properties.get("x"));
        Assert.assertEquals(record.get("y"), properties.get("y"));
        Assert.assertEquals(record.get("z"), properties.get("z"));
        Assert.assertEquals(record.get("srid"), properties.get("srid"));

        record = expected.get("geo_2d_val");
        properties = (Map) node.get("geo_2d_val");
        Assert.assertEquals(record.get("crs"), properties.get("crs"));
        Assert.assertEquals(record.get("latitude"), properties.get("latitude"));
        Assert.assertEquals(record.get("x"), properties.get("x"));
        Assert.assertEquals(record.get("y"), properties.get("y"));
        Assert.assertEquals(record.get("srid"), properties.get("srid"));
        Assert.assertEquals(record.get("longitude"), properties.get("longitude"));

        record = expected.get("geo_3d_val");
        properties = (Map) node.get("geo_3d_val");
        Assert.assertEquals(record.get("crs"), properties.get("crs"));
        Assert.assertEquals(record.get("latitude"), properties.get("latitude"));
        Assert.assertEquals(record.get("x"), properties.get("x"));
        Assert.assertEquals(record.get("y"), properties.get("y"));
        Assert.assertEquals(record.get("z"), properties.get("z"));
        Assert.assertEquals(record.get("srid"), properties.get("srid"));
        Assert.assertEquals(record.get("longitude"), properties.get("longitude"));
      }
    }
  }

  @Test
  public void testStorePropAsNodeWithRelation() throws Exception {
    ETLPlugin sourceConfig = MockSource.getPlugin("Neo4jSource", SCHEMA);

    ETLPlugin sinkConfig = new ETLPlugin(
      "Neo4jSink",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(Neo4jConstants.NAME_REFERENCE_NAME, "neo4jSinkStoreAll")
        .put(Neo4jSinkConfig.NAME_OUTPUT_QUERY, "CREATE (n:CDAP_T_1 $(string_val, date_val))-" +
          "[r:Test_Rel $(long_val, boolean_val)]->(m:CDAP_T_2 $(time_val, duration_val))")
        .build(),
      null);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT,
                                              "testStorePropertiesAAsNodeWithRelation");

    // Prepare test input data
    List<StructuredRecord> inputRecords = createInputData();
    DataSetManager<Table> inputManager = getDataset("Neo4jSource");
    MockSource.writeInput(inputManager, inputRecords);
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    String searchQuery = "MATCH (n:CDAP_T_1)-[r]->(m) RETURN n.string_val AS string_val, n.date_val AS date_val, " +
      "r.long_val AS long_val, r.boolean_val as boolean_val, m.time_val AS time_val, m.duration_val AS duration_val";
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet actual = stmt.executeQuery(searchQuery)) {
      for (StructuredRecord expected : inputRecords) {
        Assert.assertTrue(actual.next());

        CustomAssertions.assertObjectEquals(expected.get("string_val"), actual.getString("string_val"));
        CustomAssertions.assertObjectEquals(expected.getDate("date_val"),
                                            actual.getDate("date_val").toLocalDate());
        CustomAssertions.assertObjectEquals(expected.get("long_val"), actual.getLong("long_val"));
        CustomAssertions.assertObjectEquals(expected.get("boolean_val"), actual.getBoolean("boolean_val"));
        CustomAssertions.assertObjectEquals(expected.getTime("time_val").toSecondOfDay(),
                                            actual.getTime("time_val").toLocalTime().toSecondOfDay());

        StructuredRecord record = expected.get("duration_val");
        Map properties = (Map) actual.getObject("duration_val");
        Assert.assertEquals(record.get("duration"), properties.get("duration"));
        Assert.assertEquals(record.get("months"), properties.get("months"));
        Assert.assertEquals(record.get("days"), properties.get("days"));
        Assert.assertEquals(record.get("seconds"), properties.get("seconds"));
        Assert.assertEquals(record.get("nanoseconds"), properties.get("nanoseconds"));
      }
    }
  }

  private List<StructuredRecord> createInputData() throws Exception {
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    String name = "user";
    StructuredRecord.Builder builder = StructuredRecord.builder(SCHEMA)
      .set("string_val", name)
      .set("double_val", 3.451)
      .set("long_val", 125L)
      .set("boolean_val", true)
      .set("array_int", new int[]{1, 2, 3})
      .set("array_string", new String[]{"a", "b", "c"})
      .setDate("date_val", localDateTime.toLocalDate())
      .setTime("time_val", localDateTime.toLocalTime())
      .setTimestamp("timestamp_val", localDateTime.atZone(UTC_ZONE))
      .set("duration_val", StructuredRecord.builder(DURATION_SCHEMA)
        .set("duration", "P0M14DT58320S")
        .set("months", 0L)
        .set("days", 14L)
        .set("seconds", 58320L)
        .set("nanoseconds", 0)
        .build()
      )
      .set("point_2d_val", StructuredRecord.builder(POINT_2D_SCHEMA)
        .set("crs", "cartesian")
        .set("x", 3.0)
        .set("y", 0.0)
        .set("srid", 7203)
        .build()
      )
      .set("point_3d_val", StructuredRecord.builder(POINT_3D_SCHEMA)
        .set("crs", "cartesian-3d")
        .set("x", 0.0)
        .set("y", 4.0)
        .set("z", 1.0)
        .set("srid", 9157)
        .build()
      )
      .set("geo_2d_val", StructuredRecord.builder(GEO_2D_SCHEMA)
        .set("crs", "wgs-84")
        .set("latitude", 12.0)
        .set("x", 56.0)
        .set("y", 12.0)
        .set("srid", 4326)
        .set("longitude", 56.0)
        .build()
      )
      .set("geo_3d_val", StructuredRecord.builder(GEO_3D_SCHEMA)
        .set("crs", "wgs-84-3d")
        .set("latitude", 12.0)
        .set("x", 56.0)
        .set("y", 12.0)
        .set("z", 1000.0)
        .set("srid", 4979)
        .set("longitude", 56.0)
        .set("height", 1000.0)
        .build()
      );

    inputRecords.add(builder.build());

    return inputRecords;
  }

  private ETLPlugin getSinkConfig() {
    return new ETLPlugin(
      "Neo4jSink",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(Neo4jSinkConfig.NAME_OUTPUT_QUERY, "")
        .build(),
      null);
  }
}

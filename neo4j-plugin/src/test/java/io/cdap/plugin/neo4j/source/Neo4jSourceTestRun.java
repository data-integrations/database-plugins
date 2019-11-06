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

package io.cdap.plugin.neo4j.source;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.neo4j.Neo4jConstants;
import io.cdap.plugin.neo4j.NeojlPluginTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Neo4jSourceTestRun extends NeojlPluginTestBase {

  @Test
  public void testGetNodeAndRelation() throws Exception {
    String importQuery =
      "MATCH (person:CDAP_Person {name: \"Nora Ephron\"})-[rel]-(movie {title: \"Sleepless in Seattle\"}) " +
        "RETURN person, rel, movie";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(Neo4jConstants.NAME_REFERENCE_NAME, "neo4jSource")
      .putAll(BASE_PROPS)
      .put(Neo4jSourceConfig.NAME_INPUT_QUERY, importQuery)
      .put(Neo4jSourceConfig.NAME_SPLIT_NUM, "1")
      .put(Neo4jSourceConfig.NAME_ORDER_BY, "")
      .build();

    ETLPlugin sourceConfig = new ETLPlugin(
      "Neo4jSource",
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("nodeOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testNodeAndRelation");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("nodeOutputTable");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(1, records.size());

    StructuredRecord record = records.get(0);
    StructuredRecord person = record.get("person");
    StructuredRecord relation = record.get("rel");
    StructuredRecord movie = record.get("movie");

    Assert.assertEquals("Nora Ephron", person.get("name"));
    Assert.assertEquals(1941, (long) person.get("born"));
    Assert.assertEquals(1, ((ArrayList) person.get("_labels")).size());
    Assert.assertEquals("CDAP_Person", ((ArrayList) person.get("_labels")).get(0));

    Assert.assertEquals("DIRECTED", relation.get("_type"));

    Assert.assertEquals("Sleepless in Seattle", movie.get("title"));
    Assert.assertEquals(1993, (long) movie.get("released"));
    Assert.assertEquals(1, ((ArrayList) movie.get("_labels")).size());
    Assert.assertEquals("CDAP_Movie", ((ArrayList) movie.get("_labels")).get(0));
    Assert.assertEquals("What if someone you never met, someone you never saw, someone you never knew was " +
                          "the only someone for you?", movie.get("tagline"));
  }

  @Test
  public void testGetProperties() throws Exception {
    String importQuery =
      "MATCH (person)-[rel:ACTED_IN]->(movie {title: \"The Da Vinci Code\"}) " +
        "RETURN person.name AS name, person.born AS born ORDER BY born";

    List<String> names = Arrays.asList("Ian McKellen", "Tom Hanks", "Paul Bettany", "Audrey Tautou");
    List<Integer> borns = Arrays.asList(1939, 1956, 1971, 1976);

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(Neo4jConstants.NAME_REFERENCE_NAME, "neo4jSource")
      .putAll(BASE_PROPS)
      .put(Neo4jSourceConfig.NAME_INPUT_QUERY, importQuery)
      .put(Neo4jSourceConfig.NAME_SPLIT_NUM, "1")
      .put(Neo4jSourceConfig.NAME_ORDER_BY, "")
      .build();

    ETLPlugin sourceConfig = new ETLPlugin(
      "Neo4jSource",
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("propertiesOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testProperties");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("propertiesOutputTable");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(4, records.size());

    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals(names.get(i), records.get(i).get("name"));
      Assert.assertEquals((long) borns.get(i), (long) records.get(i).get("born"));
    }
  }

  @Test
  public void testSplitAndOrderBy() throws Exception {
    String importQuery =
      "MATCH (person:CDAP_Person) RETURN person.name AS name";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(Neo4jConstants.NAME_REFERENCE_NAME, "neo4jSource")
      .putAll(BASE_PROPS)
      .put(Neo4jSourceConfig.NAME_INPUT_QUERY, importQuery)
      .put(Neo4jSourceConfig.NAME_SPLIT_NUM, "3")
      .put(Neo4jSourceConfig.NAME_ORDER_BY, "person.born")
      .build();

    ETLPlugin sourceConfig = new ETLPlugin(
      "Neo4jSource",
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("splitAndOrderOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testSplitAndOrder");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("splitAndOrderOutputTable");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(11, records.size());

  }

  @Test
  public void testSplitAndOrderByInQuery() throws Exception {
    String importQuery =
      "MATCH (person:CDAP_Person) RETURN person.name AS name ORDER BY name";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(Neo4jConstants.NAME_REFERENCE_NAME, "neo4jSource")
      .putAll(BASE_PROPS)
      .put(Neo4jSourceConfig.NAME_INPUT_QUERY, importQuery)
      .put(Neo4jSourceConfig.NAME_SPLIT_NUM, "3")
      .put(Neo4jSourceConfig.NAME_ORDER_BY, "person.born")
      .build();

    ETLPlugin sourceConfig = new ETLPlugin(
      "Neo4jSource",
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("splitAndQueryOrderOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testSplitAndQueryOrder");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("splitAndQueryOrderOutputTable");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(11, records.size());

  }

  @Test
  public void testGetAllPropertiesType() throws Exception {
    String importQuery =
      "MATCH (t:CDAP_Test) RETURN t.string_val, t.long_val, t.double_val, t.boolean_val, t.date_val, t.time_val, " +
        "t.local_time_val, t.date_time_val, t.local_date_time_val, t.array_int_val, t.array_string_val, " +
        "t.duration, t.point_2d, t.point_3d, t.geo_2d, t.geo_3d";

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(Neo4jConstants.NAME_REFERENCE_NAME, "neo4jSource")
      .putAll(BASE_PROPS)
      .put(Neo4jSourceConfig.NAME_INPUT_QUERY, importQuery)
      .put(Neo4jSourceConfig.NAME_SPLIT_NUM, "1")
      .put(Neo4jSourceConfig.NAME_ORDER_BY, "")
      .build();

    ETLPlugin sourceConfig = new ETLPlugin(
      "Neo4jSource",
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("allPropertiesTypeOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testAllProperties");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    DataSetManager<Table> outputManager = getDataset("allPropertiesTypeOutputTable");
    List<StructuredRecord> records = MockSink.readOutput(outputManager);
    Assert.assertEquals(1, records.size());

    StructuredRecord structuredRecord = records.get(0);
    for (Schema.Field field : structuredRecord.getSchema().getFields()) {
      switch (field.getName()) {
        case "t_string_val":
          Assert.assertEquals("string", structuredRecord.get(field.getName()));
          break;
        case "t_long_val":
          Assert.assertEquals(123, (long) structuredRecord.get(field.getName()));
          break;
        case "t_double_val":
          Assert.assertEquals(20.32, structuredRecord.get(field.getName()), 0);
          break;
        case "t_boolean_val":
          Assert.assertEquals(true, structuredRecord.get(field.getName()));
          break;
        case "t_date_val":
          Assert.assertEquals(LocalDate.parse("2015-07-21"), structuredRecord.getDate(field.getName()));
          break;
        case "t_time_val":
          OffsetTime o = OffsetTime.parse("12:50:35.556000000+01:00");
          Assert.assertEquals(
            LocalTime.ofNanoOfDay(o.toLocalTime().toNanoOfDay() +
                                    TimeUnit.SECONDS.toNanos(o.getOffset().getTotalSeconds())).withNano(0),
            structuredRecord.getTime(field.getName()));
          break;
        case "t_local_time_val":
          Assert.assertEquals(LocalTime.parse("12:50:35.556000000").withNano(0),
                              structuredRecord.getTime(field.getName()));
          break;
        case "t_date_time_val":
          ZonedDateTime dateTime = OffsetDateTime.parse("2015-06-24T12:50:35.556000000+01:00",
                                                        DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            .atZoneSameInstant(ZoneOffset.UTC);
          Assert.assertEquals(dateTime.toLocalDateTime().atZone(ZoneId.of("UTC")),
                              structuredRecord.getTimestamp(field.getName()));
          break;
        case "t_local_date_time_val":
          Assert.assertEquals(
            Instant.from(ZonedDateTime.of(
              LocalDateTime.parse("2015-07-04T19:32:24"), ZoneId.systemDefault()))
              .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)),
            structuredRecord.getTimestamp(field.getName()));
          break;
        case "t_array_int_val":
          Assert.assertArrayEquals(Arrays.asList(1L, 2L, 3L, 4L).toArray(),
                                   ((ArrayList) structuredRecord.get(field.getName())).toArray());
          break;
        case "t_array_string_val":
          Assert.assertArrayEquals(Arrays.asList("a", "b", "c").toArray(),
                                   ((ArrayList) structuredRecord.get(field.getName())).toArray());
          break;
        case "t_duration":
          StructuredRecord duration = structuredRecord.get(field.getName());
          Assert.assertEquals("P0M14DT58320S", duration.get("duration"));
          Assert.assertEquals(0, (long) duration.get("months"));
          Assert.assertEquals(14, (long) duration.get("days"));
          Assert.assertEquals(58320, (long) duration.get("seconds"));
          Assert.assertEquals(0, (int) duration.get("nanoseconds"));
          break;
        case "t_point_2d":
          StructuredRecord point2d = structuredRecord.get(field.getName());
          Assert.assertEquals(7203, (int) point2d.get("srid"));
          Assert.assertEquals(3, (double) point2d.get("x"), 0);
          Assert.assertEquals(0, (double) point2d.get("y"), 0);
          break;
        case "t_point_3d":
          StructuredRecord point3d = structuredRecord.get(field.getName());
          Assert.assertEquals(9157, (int) point3d.get("srid"));
          Assert.assertEquals(0, (double) point3d.get("x"), 0);
          Assert.assertEquals(4, (double) point3d.get("y"), 0);
          Assert.assertEquals(1, (double) point3d.get("z"), 0);
          break;
        case "t_geo_2d":
          StructuredRecord geo2d = structuredRecord.get(field.getName());
          Assert.assertEquals(4326, (int) geo2d.get("srid"));
          Assert.assertEquals(56, (double) geo2d.get("x"), 0);
          Assert.assertEquals(12, (double) geo2d.get("y"), 0);
          break;
        case "t_geo_3d":
          StructuredRecord geo3d = structuredRecord.get(field.getName());
          Assert.assertEquals(4979, (int) geo3d.get("srid"));
          Assert.assertEquals(56, (double) geo3d.get("x"), 0);
          Assert.assertEquals(12, (double) geo3d.get("y"), 0);
          Assert.assertEquals(1000, (double) geo3d.get("z"), 0);
          break;
      }
    }
  }
}

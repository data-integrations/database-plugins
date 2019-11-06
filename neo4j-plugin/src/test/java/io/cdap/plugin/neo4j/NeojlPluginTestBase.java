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
package io.cdap.plugin.neo4j;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.batch.DatabasePluginTestBase;
import io.cdap.plugin.neo4j.sink.Neo4jDataDriveDBOutputFormat;
import io.cdap.plugin.neo4j.sink.Neo4jSink;
import io.cdap.plugin.neo4j.source.Neo4jDataDriveDBInputFormat;
import io.cdap.plugin.neo4j.source.Neo4jSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NeojlPluginTestBase extends DatabasePluginTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(NeojlPluginTestBase.class);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();
  protected static final String DRIVER_CLASS = "org.neo4j.jdbc.bolt.BoltDriver";
  protected static final String JDBC_DRIVER_NAME = "neo4j";
  protected static final ZoneId UTC_ZONE = ZoneId.ofOffset("UTC", ZoneOffset.UTC);

  protected static String connectionUrl;
  protected static boolean tearDown = true;
  private static int startCount;
  private static final List<String> QUERY = Arrays.asList(
    "CREATE (SleeplessInSeattle:CDAP_Movie {title:'Sleepless in Seattle', released:1993, " +
    "tagline:'What if someone you never met, someone you never saw, someone you never knew was the only " +
    "someone for you?'})",
    "CREATE (TheDaVinciCode:CDAP_Movie {title:'The Da Vinci Code', released:2006, tagline:'Break The Codes'})",

    "CREATE (TomH:CDAP_Person {name:'Tom Hanks', born:1956})",
    "CREATE (MegR:CDAP_Person {name:'Meg Ryan', born:1961})",
    "CREATE (RitaW:CDAP_Person {name:'Rita Wilson', born:1956})",
    "CREATE (BillPull:CDAP_Person {name:'Bill Pullman', born:1953})",
    "CREATE (VictorG:CDAP_Person {name:'Victor Garber', born:1949})",
    "CREATE (RosieO:CDAP_Person {name:\"Rosie O'Donnell\", born:1962})",
    "CREATE (NoraE:CDAP_Person {name:'Nora Ephron', born:1941})",
    "CREATE (IanM:CDAP_Person {name:'Ian McKellen', born:1939})",
    "CREATE (AudreyT:CDAP_Person {name:'Audrey Tautou', born:1976})",
    "CREATE (PaulB:CDAP_Person {name:'Paul Bettany', born:1971})",
    "CREATE (RonH:CDAP_Person {name:'Ron Howard', born:1954})",

    "MATCH (TomH {name:'Tom Hanks'}), (MegR {name:'Meg Ryan'}), (RitaW {name:'Rita Wilson'})," +
      "(BillPull {name:'Bill Pullman'}), (VictorG {name:'Victor Garber'}), (RosieO {name:\"Rosie O'Donnell\"})," +
      "(NoraE {name:'Nora Ephron'}), (SleeplessInSeattle {title:'Sleepless in Seattle'}) " +
      "CREATE (TomH)-[:ACTED_IN {roles:['Sam Baldwin']}]->(SleeplessInSeattle), " +
      "(MegR)-[:ACTED_IN {roles:['Annie Reed']}]->(SleeplessInSeattle), " +
      "(RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle), " +
      "(BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle), " +
      "(VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle), " +
      "(RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle), " +
      "(NoraE)-[:DIRECTED]->(SleeplessInSeattle)",

    "MATCH (TomH {name:'Tom Hanks'}), (IanM {name:'Ian McKellen'}), (AudreyT {name:'Audrey Tautou'}), " +
      "(PaulB {name:'Paul Bettany'}), (RonH {name:'Ron Howard'}), (TheDaVinciCode {title:'The Da Vinci Code'}) " +
      "CREATE (TomH)-[:ACTED_IN {roles:['Dr. Robert Langdon']}]->(TheDaVinciCode), " +
      "(IanM)-[:ACTED_IN {roles:['Sir Leight Teabing']}]->(TheDaVinciCode), " +
      "(AudreyT)-[:ACTED_IN {roles:['Sophie Neveu']}]->(TheDaVinciCode), " +
      "(PaulB)-[:ACTED_IN {roles:['Silas']}]->(TheDaVinciCode), " +
      "(RonH)-[:DIRECTED]->(TheDaVinciCode)",

    "CREATE (t:CDAP_Test {string_val: \"string\", long_val: 123, double_val: 20.32, boolean_val: true, " +
      "date_val: date('2015-07-21'), time_val: time('125035.556+0100'), local_time_val: localtime('12:50:35.556'), " +
      "date_time_val: datetime('2015-06-24T12:50:35.556+0100'), " +
      "local_date_time_val: localdatetime('2015185T19:32:24'), array_int_val: [1,2,3,4], " +
      "array_string_val: ['a','b','c'], duration: duration(\"P14DT16H12M\"), " +
      "point_2d: point({ x:3, y:0 }), point_3d: point({ x:0, y:4, z:1 }), " +
      "geo_2d: point({ latitude: 12, longitude: 56 }), geo_3d: point({ latitude: 12, longitude: 56, height: 1000 })})");


  private static final List<String> DROP_QUERY_LIST = Arrays.asList(
    "MATCH (p:CDAP_Person), (m:CDAP_Movie), (t:CDAP_Test) DETACH DELETE p, m, t",
    "MATCH (ts:CDAP_TEST_1) DETACH DELETE ts",
    "MATCH (ts1: CDAP_T_1), (ts2:CDAP_T_2) DETACH DELETE ts1, ts2"
  );

  protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
    .put(Neo4jConstants.NAME_HOST_STRING, System.getProperty("neo4j.host", "localhost"))
    .put(Neo4jConstants.NAME_PORT_STRING, System.getProperty("neo4j.port", "7687"))
    .put(Neo4jConstants.NAME_USERNAME, System.getProperty("neo4j.username", "neo4j"))
    .put(Neo4jConstants.NAME_PASSWORD, System.getProperty("neo4j.password", "test"))
    .build();

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact(JDBC_DRIVER_NAME, "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Neo4jSource.class, Neo4jSink.class, Neo4jDataDriveDBOutputFormat.class,
                      Neo4jDataDriveDBInputFormat.class, Neo4jRecord.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    // add neo4j 3rd party plugin
    PluginClass neo4jDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                                              "neoj driver class",
                                              driverClass.getName(),
                                              null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("neo4j-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(neo4jDriver), driverClass);


    connectionUrl = "jdbc:neo4j:bolt://" + BASE_PROPS.get(Neo4jConstants.NAME_HOST_STRING) + ":" +
      BASE_PROPS.get(Neo4jConstants.NAME_PORT_STRING) + "/?username=" +
      BASE_PROPS.get(Neo4jConstants.NAME_USERNAME) + ",password=" + BASE_PROPS.get(Neo4jConstants.NAME_PASSWORD);
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      createTestData(stmt);
    }
  }

  private static void createTestData(Statement stmt) throws SQLException {
    for (String query : QUERY) {
      LOG.debug("run -> " + query);
      stmt.execute(query);
    }
  }

  public static Connection createConnection() {
    try {
      Class.forName(DRIVER_CLASS);
      return DriverManager.getConnection(connectionUrl);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @AfterClass
  public static void tearDownDB() throws SQLException {
    if (!tearDown) {
      return;
    }

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      for (String query : DROP_QUERY_LIST) {
        stmt.execute(query);
      }
    }
  }
}

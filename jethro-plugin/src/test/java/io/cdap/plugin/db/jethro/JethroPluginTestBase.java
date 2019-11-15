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

package io.cdap.plugin.db.jethro;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.batch.DatabasePluginTestBase;
import io.cdap.plugin.db.batch.source.DataDrivenETLDBInputFormat;
import io.cdap.plugin.db.jethro.source.JethroSource;
import io.cdap.plugin.db.jethro.source.JethroSourceConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;

public class JethroPluginTestBase extends DatabasePluginTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(JethroPluginTestBase.class);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();
  protected static final String DRIVER_CLASS = "com.jethrodata.JethroDriver";
  protected static final String JDBC_DRIVER_NAME = "jethro";

  protected static String connectionUrl;
  protected static final ZoneId UTC_ZONE = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
  protected static boolean tearDown = true;
  private static int startCount;

  protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
    .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
    .put(ConnectionConfig.HOST, System.getProperty("jethro.host", "localhost"))
    .put(ConnectionConfig.PORT, System.getProperty("jethro.port", "9112"))
    .put(ConnectionConfig.DATABASE, System.getProperty("jethro.instance", "demo"))
    .put(ConnectionConfig.USER, System.getProperty("jethro.username", "jethro"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("jethro.password", "jethro"))
    .build();

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact(JDBC_DRIVER_NAME, "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID, JethroSource.class, JethroSourceConfig.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    // add jethro 3rd party plugin
    PluginClass jethroDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                                              "jethro driver class",
                                              driverClass.getName(),
                                              null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("jethro-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(jethroDriver), driverClass);


    connectionUrl = "jdbc:JethroData://" + BASE_PROPS.get(ConnectionConfig.HOST) + ":" +
      BASE_PROPS.get(ConnectionConfig.PORT) + "/" + BASE_PROPS.get(ConnectionConfig.DATABASE);
    Connection conn = createConnection();
    createTestTables(conn);
    populateData(conn);
  }

  public static Connection createConnection() {
    try {
      Class.forName(DRIVER_CLASS);
      return DriverManager.getConnection(connectionUrl, BASE_PROPS.get(ConnectionConfig.USER),
                                         BASE_PROPS.get(ConnectionConfig.PASSWORD));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE test_table (int_value int, long_value bigint, float_value float, " +
                     "double_value double, timestamp_value timestamp, string_value string)");
    }
  }

  private static void populateData(Connection conn) throws SQLException {

    try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?,?,?,?,?)")) {
      for (int i = 0; i < 4; i++) {
        stmt.setInt(1, 1 + i);
        stmt.setLong(2, (long) 100 + i);
        stmt.setFloat(3, (float) 0.1 + i);
        stmt.setDouble(4, 0.03 + i);
        stmt.setTimestamp(5, new Timestamp(CURRENT_TS));
        stmt.setString(6, "Test_" + i);
        stmt.execute();
      }
    }

    try (PreparedStatement stmt =
           conn.prepareStatement(
             "INSERT INTO test_table (int_value, long_value, float_value, double_value) VALUES (?,?,?,?)")) {
        stmt.setInt(1, 5);
        stmt.setLong(2, (long) 104);
        stmt.setFloat(3, (float) 4.1);
        stmt.setDouble(4, 4.03);
        stmt.execute();
    }
  }

  @AfterClass
  public static void tearDownDB() throws SQLException {
    if (!tearDown) {
      return;
    }

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE test_table");
    }
  }
}

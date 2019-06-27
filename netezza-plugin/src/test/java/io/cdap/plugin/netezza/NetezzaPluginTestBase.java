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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.batch.DatabasePluginTestBase;
import io.cdap.plugin.db.batch.sink.ETLDBOutputFormat;
import io.cdap.plugin.db.batch.source.DataDrivenETLDBInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;

public class NetezzaPluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();

  protected static final String JDBC_DRIVER_NAME = "netezza";

  protected static String connectionUrl;
  protected static final int YEAR;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  protected static boolean tearDown = true;
  private static int startCount;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  static {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date(CURRENT_TS));
    YEAR = calendar.get(Calendar.YEAR);
  }

  protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
    .put(ConnectionConfig.HOST, System.getProperty("netezza.host"))
    .put(ConnectionConfig.PORT, System.getProperty("netezza.port"))
    .put(ConnectionConfig.DATABASE, System.getProperty("netezza.database"))
    .put(ConnectionConfig.USER, System.getProperty("netezza.username"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("netezza.password"))
    .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
    .build();

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact(JDBC_DRIVER_NAME, "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      NetezzaSource.class, NetezzaSink.class, DBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class, NetezzaPostAction.class, NetezzaAction.class);

    connectionUrl = "jdbc:netezza://" + BASE_PROPS.get(ConnectionConfig.HOST) + ":" +
      BASE_PROPS.get(ConnectionConfig.PORT) + "/" + BASE_PROPS.get(ConnectionConfig.DATABASE);

    Class<?> aClass = Class.forName("org.netezza.Driver");

    // add netezza 3rd party plugin
    PluginClass netezzaDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                                                "netezza driver class", aClass.getCanonicalName(),
                                                null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("netezza-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(netezzaDriver), aClass);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    Connection conn = createConnection();
    createTestTables(conn);
    prepareTestData(conn);
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE db_action_test (x int, day varchar(10))");
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE post_action_test (x int, day varchar(10))");

      stmt.execute("CREATE TABLE my_table (" +
                     "ID INT NOT NULL," +
                     "NAME VARCHAR(40) NOT NULL," +
                     "SCORE REAL," +
                     "GRADUATED BOOLEAN," +
                     "NOT_IMPORTED VARCHAR(30)," +
                     "BYTEINT_COL BYTEINT," +
                     "SMALLINT_COL SMALLINT," +
                     "INTEGER_COL INTEGER," +
                     "BIGINT_COL BIGINT," +
                     "CHAR_COL CHARACTER(40)," +
                     "VARCHAR_COL CHARACTER VARYING(40)," +
                     "NCHAR_COL NATIONAL CHARACTER(40)," +
                     "DATE_COL DATE," +
                     "TIME_COL TIME," +
                     "TIME_WITH_TIME_ZONE_COL TIME WITH TIME ZONE," +
                     "TIMESTAMP_COL TIMESTAMP," +
                     "INTERVAL_COL INTERVAL," +
                     "DOUBLE_PRECISION_COL DOUBLE PRECISION," +
                     "NUMERIC_COL NUMERIC(" + PRECISION + "," + SCALE + ")," +
                     "NVARCHAR_COL NATIONAL CHARACTER VARYING(40)," +
                     "REAL_COL REAL," +
                     "ST_GEOMETRY_COL ST_GEOMETRY(10)," +
                     "VARBINARY_COL BINARY VARYING(10)," +
                     "DECIMAL_COL DECIMAL(" + PRECISION + "," + SCALE + ")," +
                     "FLOAT_COL FLOAT(6))");
      stmt.execute("CREATE TABLE MY_DEST_TABLE AS " +
                     "SELECT * FROM my_table");
      stmt.execute("CREATE TABLE your_table AS " +
                     "SELECT * FROM my_table");
    }
  }

  protected static void prepareTestData(Connection conn) throws SQLException {
    try (
      Statement stmt = conn.createStatement();
      PreparedStatement pStmt1 =
        conn.prepareStatement("INSERT INTO my_table " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "       ?, ?, ?, ?, ?)");
      PreparedStatement pStmt2 =
        conn.prepareStatement("INSERT INTO your_table " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "       ?, ?, ?, ?, ?)")) {

      stmt.execute("insert into db_action_test values (1, '1970-01-01')");
      stmt.execute("insert into post_action_test values (1, '1970-01-01')");

      populateData(pStmt1, pStmt2);
    }
  }

  private static void populateData(PreparedStatement ...stmts) throws SQLException {
    // insert the same data into both tables: my_table and your_table
    for (PreparedStatement pStmt : stmts) {
      for (int i = 1; i <= 5; i++) {
        String name = "user" + i;
        pStmt.setInt(1, i);
        pStmt.setString(2, name);
        pStmt.setDouble(3, 123.45 + i);
        pStmt.setBoolean(4, (i % 2 == 0));
        pStmt.setString(5, "random" + i);
        pStmt.setShort(6, (short) i);
        pStmt.setShort(7, (short) i);
        pStmt.setInt(8, i);
        pStmt.setLong(9, (long) i);
        pStmt.setString(10, name);
        pStmt.setString(11, name);
        pStmt.setString(12, name);
        pStmt.setDate(13, new Date(CURRENT_TS));
        pStmt.setTime(14, new Time(CURRENT_TS));
        pStmt.setTime(15, new Time(CURRENT_TS));
        pStmt.setTimestamp(16, new Timestamp(CURRENT_TS));
        pStmt.setString(17, "2 year 3 month " + i + " day");
        pStmt.setDouble(18, 123.45 + i);
        pStmt.setBigDecimal(19, new BigDecimal(123.45).add(new BigDecimal(i)));
        pStmt.setString(20, name);
        pStmt.setDouble(21, 123.45 + i);
        pStmt.setBytes(22, name.getBytes(Charsets.UTF_8));
        pStmt.setBytes(23, name.getBytes(Charsets.UTF_8));
        pStmt.setBigDecimal(24, new BigDecimal(123.45).add(new BigDecimal(i)));
        pStmt.setFloat(25, (float) 123.45 + i);

        pStmt.executeUpdate();
      }
    }
  }

  public static Connection createConnection() {
    try {
      Class.forName(Driver.class.getCanonicalName());
      return DriverManager.getConnection(connectionUrl, BASE_PROPS.get(ConnectionConfig.USER),
                                         BASE_PROPS.get(ConnectionConfig.PASSWORD));
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
      stmt.execute("DROP TABLE my_table");
      stmt.execute("DROP TABLE your_table");
      stmt.execute("DROP TABLE post_action_test");
      stmt.execute("DROP TABLE db_action_test");
      stmt.execute("DROP TABLE MY_DEST_TABLE");
    }
  }
}

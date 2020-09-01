/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.teradata;

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
import io.cdap.plugin.teradata.action.TeradataAction;
import io.cdap.plugin.teradata.postaction.TeradataPostAction;
import io.cdap.plugin.teradata.sink.TeradataSink;
import io.cdap.plugin.teradata.source.TeradataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;

public abstract class TeradataPluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();

  protected static final String JDBC_DRIVER_NAME = "teradata";
  protected static final String DRIVER_CLASS = "com.teradata.jdbc.TeraDriver";

  protected static String connectionUrl;
  protected static final int YEAR;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  protected static final ZoneId UTC_ZONE = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
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
    .put(ConnectionConfig.HOST, System.getProperty("teradata.host", "localhost"))
    .put(ConnectionConfig.PORT, System.getProperty("teradata.port", "1025"))
    .put(ConnectionConfig.DATABASE, System.getProperty("teradata.database", "mydb"))
    .put(ConnectionConfig.USER, System.getProperty("teradata.username", "test"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("teradata.password", "test"))
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
                      TeradataSource.class, TeradataSink.class, TeradataDBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class, TeradataAction.class, TeradataPostAction.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    // add mysql 3rd party plugin
    PluginClass teradtaDriver = new PluginClass(
      ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME, "teradata driver class",
      driverClass.getName(),
      null,
      Collections.emptyMap()
    );

    addPluginArtifact(NamespaceId.DEFAULT.artifact("teradata-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(teradtaDriver), driverClass);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    connectionUrl = TeradataUtils.getConnectionString(
      BASE_PROPS.get(ConnectionConfig.HOST),
      Integer.parseInt(BASE_PROPS.get(ConnectionConfig.PORT)),
      BASE_PROPS.get(ConnectionConfig.DATABASE),
      ""
    );
    Connection conn = createConnection();

    createTestTables(conn);
    prepareTestData(conn);
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String columns = "ID INTEGER NOT NULL, " +
        "NAME VARCHAR(40) NOT NULL, " +
        "SCORE DOUBLE PRECISION, " +
        "DATE_COL DATE, " +
        "GRADUATED BYTEINT, " +
        "NOT_IMPORTED VARCHAR(30), " +
        "CHAR_COL CHAR(100)," +
        "VARCHAR_COL VARCHAR(100)," +
        "CLOB_COL CLOB," +
        "BINARY_COL BYTE(5)," +
        "VARBINARY_COL VARBYTE(20)," +
        "BLOB_COL BLOB, " +
        "SMALL SMALLINT, " +
        "BIG BIGINT, " +
        "DECIMAL_COL DECIMAL(" + PRECISION + "," + SCALE + "), " +
        "NUMBER_COL NUMBER(" + PRECISION + "," + SCALE + "), " +
        "TIME_COL TIME, " +
        "TIMESTAMP_COL TIMESTAMP, " +
        "TIMETZ_COL TIME With Time Zone, " +
        "TIMESTAMPTZ_COL TIMESTAMP WITH TIME ZONE, " +
        "INTERVAL_YEAR_COL INTERVAL YEAR (4), " +
        "INTERVAL_YEAR_TO_MONTH_COL INTERVAL YEAR(4) TO MONTH, " +
        "INTERVAL_MONTH_COL INTERVAL MONTH(2), " +
        "INTERVAL_DAY_COL INTERVAL DAY(2), " +
        "INTERVAL_DAY_TO_HOUR_COL INTERVAL DAY(2) TO HOUR, " +
        "INTERVAL_DAY_TO_MINUTE_COL INTERVAL DAY(2) TO MINUTE, " +
        "INTERVAL_DAY_TO_SECOND_COL INTERVAL DAY(2) TO SECOND(3), " +
        "INTERVAL_HOUR_COL INTERVAL HOUR(2), " +
        "INTERVAL_HOUR_TO_MINUTE_COL INTERVAL HOUR(2) TO MINUTE, " +
        "INTERVAL_HOUR_TO_SECOND_COL INTERVAL HOUR(2) TO SECOND(3), " +
        "INTERVAL_MINUTE_COL INTERVAL MINUTE(2), " +
        "INTERVAL_MINUTE_TO_SECOND_COL INTERVAL MINUTE(2) TO SECOND(3), " +
        "INTERVAL_SECOND_COL INTERVAL SECOND(2,3), " +
        "ST_GEOMETRY_COL ST_Geometry";

      stmt.execute("CREATE TABLE my_table(" + columns + ")");
      stmt.execute("CREATE TABLE your_table(" + columns + ")");
      stmt.execute("CREATE TABLE MY_DEST_TABLE(" + columns + ")");

      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE dbActionTest (x int, \"day\" varchar(10))");
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE postActionTest (x INT, \"day\" VARCHAR(10))");
    }
  }

  protected static void prepareTestData(Connection conn) throws SQLException {
    String insertTemplate = "INSERT INTO %s VALUES(" +
      "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
      "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
      ")";

    try (
      Statement stmt = conn.createStatement();

      PreparedStatement pStmt1 =
        conn.prepareStatement(String.format(insertTemplate, "my_table"));
      PreparedStatement pStmt2 =
        conn.prepareStatement(String.format(insertTemplate, "your_table"))) {

      stmt.execute("insert into dbActionTest values (1, '1970-01-01')");
      stmt.execute("insert into postActionTest values (1, '1970-01-01')");

      populateData(pStmt1, pStmt2);
    }
  }

  private static void populateData(PreparedStatement... stmts) throws SQLException {
    // insert the same data into both tables: my_table and your_table
    for (PreparedStatement pStmt : stmts) {
      for (int i = 1; i <= 5; i++) {
        String name = "user" + i;
        pStmt.setInt(1, i);
        pStmt.setString(2, name);
        pStmt.setDouble(3, 123.45 + i);
        pStmt.setDate(4, new Date(CURRENT_TS));
        pStmt.setBoolean(5, (i % 2 == 0));
        pStmt.setString(6, "random" + i);
        pStmt.setString(7, name);
        pStmt.setString(8, name);
        pStmt.setString(9, name);
        pStmt.setBytes(10, name.getBytes());
        pStmt.setBytes(11, name.getBytes());
        pStmt.setBytes(12, name.getBytes());
        pStmt.setShort(13, (short) i);
        pStmt.setLong(14, (long) i);
        pStmt.setBigDecimal(15, new BigDecimal(123.45).add(new BigDecimal(i)));
        pStmt.setBigDecimal(16, new BigDecimal(54.65).add(new BigDecimal(i)));
        pStmt.setTime(17, new Time(CURRENT_TS));
        pStmt.setTimestamp(18, new Timestamp(CURRENT_TS));
        pStmt.setTime(19, new Time(CURRENT_TS));
        pStmt.setTimestamp(20, new Timestamp(CURRENT_TS));
        pStmt.setString(21, "2019");
        pStmt.setString(22, "2019-10");
        pStmt.setString(23, "10");
        pStmt.setString(24, "11");
        pStmt.setString(25, "11 12");
        pStmt.setString(26, "11 12:13");
        pStmt.setString(27, "11 12:13:14.567");
        pStmt.setString(28, "12");
        pStmt.setString(29, "12:13");
        pStmt.setString(30, "12:13:14.567");
        pStmt.setString(31, "13");
        pStmt.setString(32, "13:14.567");
        pStmt.setString(33, "14.567");
        pStmt.setString(34, "POINT (10 20)");
        pStmt.executeUpdate();
      }
    }
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

  @AfterClass
  public static void tearDownDB() throws SQLException {
    if (!tearDown) {
      return;
    }

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE my_table");
      stmt.execute("DROP TABLE your_table");
      stmt.execute("DROP TABLE MY_DEST_TABLE");
      stmt.execute("DROP TABLE postActionTest");
      stmt.execute("DROP TABLE dbActionTest");
    }
  }
}

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

package io.cdap.plugin.db2;

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
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

public class Db2PluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();
  private static final String DRIVER_CLASS = "com.ibm.db2.jcc.DB2Driver";
  protected static final String JDBC_DRIVER_NAME = "db211";

  protected static String connectionUrl;
  protected static int year;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  protected static boolean tearDown = true;
  private static int startCount;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  static {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date(CURRENT_TS));
    year = calendar.get(Calendar.YEAR);
  }

  protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
    .put(ConnectionConfig.HOST, System.getProperty("db2.host"))
    .put(ConnectionConfig.PORT, System.getProperty("db2.port"))
    .put(ConnectionConfig.DATABASE, System.getProperty("db2.database"))
    .put(ConnectionConfig.USER, System.getProperty("db2.username"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("db2.password"))
    .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
    .build();

  @BeforeClass
  public static void setupTest() throws Exception {

    if (startCount++ > 0) {
      return;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date(CURRENT_TS));
    year = calendar.get(Calendar.YEAR);
    
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact(JDBC_DRIVER_NAME, "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Db2Source.class, Db2Sink.class, DBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class, Db2PostAction.class, Db2Action.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    // add DB2 3rd party plugin
    PluginClass oracleDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME, "DB2 driver class",
                                           driverClass.getName(),
                                           null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("db2-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(oracleDriver), driverClass);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    connectionUrl = "jdbc:db2://" + BASE_PROPS.get(ConnectionConfig.HOST) + ":" +
      BASE_PROPS.get(ConnectionConfig.PORT) + "/" + BASE_PROPS.get(ConnectionConfig.DATABASE);
    Connection conn = createConnection();
    createTestTables(conn);
    prepareTestData(conn);
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE dbActionTest (x int, day varchar(10))");
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE postActionTest (x int, day varchar(10))");

      stmt.execute("CREATE TABLE my_table (" +
                     "  SMALLINT_COL SMALLINT," +
                     "  INTEGER_COL INTEGER," +
                     "  BIGINT_COL BIGINT," +
                     "  DECIMAL_COL DECIMAL(" + PRECISION + "," + SCALE + ")," +
                     "  NUMERIC_COL NUMERIC(" + PRECISION + "," + SCALE + ")," +
                     "  DECFLOAT_COL DECFLOAT," +
                     "  REAL_COL REAL," +
                     "  DOUBLE_COL DOUBLE," +
                     "  CHAR_COL CHAR(10)," +
                     "  VARCHAR_COL VARCHAR(10)," +
                     "  CHAR_BIT_COL CHAR(10) FOR BIT DATA," +
                     "  VARCHAR_BIT_COL VARCHAR(10) FOR BIT DATA," +
                     "  GRAPHIC_COL GRAPHIC(10)," +
                     "  CLOB_COL CLOB," +
                     "  BLOB_COL BLOB," +
                     "  DATE_COL DATE," +
                     "  TIME_COL TIME," +
                     "  TIMESTAMP_COL TIMESTAMP" +
                     ")");
      stmt.execute("CREATE TABLE MY_DEST_TABLE LIKE my_table");
      stmt.execute("CREATE TABLE your_table LIKE my_table");
    }
  }

  protected static void prepareTestData(Connection conn) throws SQLException {
    try (
      Statement stmt = conn.createStatement();
      PreparedStatement pStmt1 =
        conn.prepareStatement("INSERT INTO my_table " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "       ?, ?, ?, ?, ?, ?, ?, ?)");
      PreparedStatement pStmt2 =
        conn.prepareStatement("INSERT INTO your_table " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "       ?, ?, ?, ?, ?, ?, ?, ?)")) {

      stmt.execute("insert into dbActionTest values (1, '1970-01-01')");
      stmt.execute("insert into postActionTest values (1, '1970-01-01')");

      populateData(pStmt1, pStmt2);
    }
  }

  private static void populateData(PreparedStatement ...stmts) throws SQLException {
    // insert the same data into both tables: my_table and your_table
    for (PreparedStatement pStmt : stmts) {
      for (int i = 1; i <= 5; i++) {
          String name = "user" + i;
          pStmt.setShort(1, (short) i);
          pStmt.setInt(2, i);
          pStmt.setLong(3, (long) i);
          pStmt.setBigDecimal(4, new BigDecimal(3.14).add(new BigDecimal(i)));
          pStmt.setBigDecimal(5, new BigDecimal(3.14).add(new BigDecimal(i)));
          pStmt.setBigDecimal(6, new BigDecimal(3.14).add(new BigDecimal(i)));
          pStmt.setFloat(7, i + 3.14f);
          pStmt.setDouble(8, i + 3.14);
          pStmt.setString(9, name);
          pStmt.setString(10, name);
          pStmt.setBytes(11, name.getBytes());
          pStmt.setBytes(12, name.getBytes());
          pStmt.setString(13, name);
          pStmt.setClob(14, new SerialClob(name.toCharArray()));
          pStmt.setBlob(15, new SerialBlob(name.getBytes()));
          pStmt.setDate(16, new Date(CURRENT_TS));
          pStmt.setTime(17, new Time(CURRENT_TS));
          pStmt.setTimestamp(18, new Timestamp(CURRENT_TS));

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
      stmt.execute("DROP TABLE postActionTest");
      stmt.execute("DROP TABLE dbActionTest");
      stmt.execute("DROP TABLE MY_DEST_TABLE");
    }
  }
}

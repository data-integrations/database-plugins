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

package io.cdap.plugin.postgres;

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
import org.postgresql.Driver;

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

public class PostgresPluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();

  protected static final String JDBC_DRIVER_NAME = "postrgesql";

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
    .put(ConnectionConfig.HOST, System.getProperty("postgresql.host"))
    .put(ConnectionConfig.PORT, System.getProperty("postgresql.port"))
    .put(ConnectionConfig.DATABASE, System.getProperty("postgresql.database"))
    .put(ConnectionConfig.USER, System.getProperty("postgresql.username"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("postgresql.password"))
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
                      PostgresSource.class, PostgresSink.class, DBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class, PostgresPostAction.class, PostgresAction.class);

    // add mysql 3rd party plugin
    PluginClass mysqlDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                                              "postrgesql driver class", Driver.class.getName(),
                                              null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("postrgesql-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(mysqlDriver), Driver.class);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    connectionUrl = "jdbc:postgresql://" + BASE_PROPS.get(ConnectionConfig.HOST) + ":" +
      BASE_PROPS.get(ConnectionConfig.PORT) + "/" + BASE_PROPS.get(ConnectionConfig.DATABASE);
    Connection conn = createConnection();
    createTestTables(conn);
    prepareTestData(conn);
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE \"dbActionTest\" (x int, day varchar(10))");
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE \"postActionTest\" (x int, day varchar(10))");

      stmt.execute("CREATE TABLE my_table" +
                     "(" +
                     "\"ID\" INT NOT NULL," +
                     "\"NAME\" VARCHAR(40) NOT NULL," +
                     "\"SCORE\" REAL," +
                     "\"GRADUATED\" BOOLEAN," +
                     "\"NOT_IMPORTED\" VARCHAR(30)," +
                     "\"SMALLINT_COL\" SMALLINT," +
                     "\"BIG\" BIGINT," +
                     "\"NUMERIC_COL\" NUMERIC(" + PRECISION + "," + SCALE + ")," +
                     "\"DECIMAL_COL\" DECIMAL(" + PRECISION + "," + SCALE + ")," +
                     "\"DOUBLE_PREC_COL\" DOUBLE PRECISION," +
                     "\"DATE_COL\" DATE," +
                     "\"TIME_COL\" TIME," +
                     "\"TIMESTAMP_COL\" TIMESTAMP(3)," +
                     "\"TEXT_COL\" TEXT," +
                     "\"CHAR_COL\" CHAR(100)," +
                     "\"BYTEA_COL\" BYTEA" +
                     ")");
      stmt.execute("CREATE TABLE \"MY_DEST_TABLE\" AS " +
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
                                "       ?, ?, ?, ?, ?, ?)");
      PreparedStatement pStmt2 =
        conn.prepareStatement("INSERT INTO your_table " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "       ?, ?, ?, ?, ?, ?)")) {

      stmt.execute("insert into \"dbActionTest\" values (1, '1970-01-01')");
      stmt.execute("insert into \"postActionTest\" values (1, '1970-01-01')");

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
        pStmt.setLong(7, (long) i);
        pStmt.setBigDecimal(8, new BigDecimal(123.45).add(new BigDecimal(i)));
        pStmt.setBigDecimal(9, new BigDecimal(123.45).add(new BigDecimal(i)));
        pStmt.setDouble(10, 123.45 + i);
        pStmt.setDate(11, new Date(CURRENT_TS));
        pStmt.setTime(12, new Time(CURRENT_TS));
        pStmt.setTimestamp(13, new Timestamp(CURRENT_TS));
        pStmt.setString(14, name);
        pStmt.setString(15, "char" + i);
        pStmt.setBytes(16, name.getBytes(Charsets.UTF_8));
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
      stmt.execute("DROP TABLE \"postActionTest\"");
      stmt.execute("DROP TABLE \"dbActionTest\"");
      stmt.execute("DROP TABLE \"MY_DEST_TABLE\"");
    }
  }
}

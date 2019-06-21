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

package io.cdap.plugin.mssql;

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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;

public class SqlServerPluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();
  protected static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  protected static final String JDBC_DRIVER_NAME = "sqlserver42";

  protected static String connectionUrl;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  protected static boolean tearDown = true;
  private static int startCount;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
    .put(ConnectionConfig.HOST, System.getProperty("mssql.host"))
    .put(ConnectionConfig.PORT, System.getProperty("mssql.port"))
    .put(ConnectionConfig.DATABASE, System.getProperty("mssql.database"))
    .put(ConnectionConfig.USER, System.getProperty("mssql.username"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("mssql.password"))
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
                      SqlServerSource.class, SqlServerSink.class, DBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, SqlServerPostAction.class, SqlServerAction.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    // add SqlServer 3rd party plugin
    PluginClass sqlServerDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                                                  "sql server driver class", driverClass.getName(),
                                                  null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("sqlserver-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(sqlServerDriver), driverClass);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    connectionUrl = "jdbc:sqlserver://" + BASE_PROPS.get(ConnectionConfig.HOST) + ":" +
      BASE_PROPS.get(ConnectionConfig.PORT) + ";databaseName=" + BASE_PROPS.get(ConnectionConfig.DATABASE);
    try (Connection conn = createConnection()) {
      conn.setAutoCommit(false);
      try {
        createTestTables(conn);
        prepareTestData(conn);
        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE dbActionTest (x int, day varchar(10))");
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE postActionTest (x int, day varchar(10))");

      stmt.execute("CREATE TABLE my_table" +
                     "(" +
                     "ID INT NOT NULL, " +
                     "NAME VARCHAR(40) NOT NULL, " +
                     "NOT_IMPORTED VARCHAR(30), " +
                     "TINY TINYINT, " +
                     "SMALL SMALLINT, " +
                     "BIG BIGINT, " +
                     "FLOAT_COL FLOAT, " +
                     "REAL_COL REAL, " +
                     "NUMERIC_COL NUMERIC(" + PRECISION + "," + SCALE + "), " +
                     "DECIMAL_COL DECIMAL(" + PRECISION + "," + SCALE + "), " +
                     "BIT_COL BIT, " +
                     "DATE_COL DATE, " +
                     "DATETIME_COL DATETIME2, " +
                     "SMALLDATETIME_COL SMALLDATETIME, " +
                     "TIME_COL TIME, " +
                     "IMAGE_COL IMAGE, " +
                     "MONEY_COL MONEY, " +
                     "SMALLMONEY_COL SMALLMONEY, " +
                     "NCHAR_COL NCHAR(100), " +
                     "NTEXT_COL NTEXT, " +
                     "NVARCHAR_COL NVARCHAR(100), " +
                     "TEXT_COL TEXT," +
                     "CHAR_COL CHAR(100)," +
                     "BINARY_COL BINARY(100)," +
                     "VARBINARY_COL VARBINARY(20)" +
                     ")");
      stmt.execute("SELECT * INTO MY_DEST_TABLE FROM my_table");
      stmt.execute("SELECT * INTO your_table FROM my_table");
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
        pStmt.setString(3, name);
        pStmt.setByte(4, (byte) i);
        pStmt.setShort(5, (short) i);
        pStmt.setLong(6, (long) i);
        pStmt.setDouble(7, 123.45 + i);
        pStmt.setFloat(8, 123.45f + i);
        pStmt.setBigDecimal(9, new BigDecimal(123.45).add(new BigDecimal(i)));

        if ((i % 2 == 0)) {
          pStmt.setNull(10, Types.DECIMAL);
        } else {
          pStmt.setBigDecimal(10, new BigDecimal(123.45).add(new BigDecimal(i)));
        }


        pStmt.setBoolean(11, (i % 2 == 0));
        pStmt.setDate(12, new Date(CURRENT_TS));
        pStmt.setTimestamp(13, new Timestamp(CURRENT_TS));
        pStmt.setTimestamp(14, new Timestamp(CURRENT_TS));
        pStmt.setTime(15, new Time(CURRENT_TS));
        pStmt.setBytes(16, name.getBytes());
        pStmt.setBigDecimal(17, new BigDecimal(123.45).add(new BigDecimal(i)));
        pStmt.setFloat(18, 123.45f + (short) i);
        pStmt.setString(19, name);
        pStmt.setString(20, name);
        pStmt.setString(21, name);
        pStmt.setString(22, name);
        pStmt.setString(23, "char" + i);
        pStmt.setBytes(24, name.getBytes(Charsets.UTF_8));
        pStmt.setBytes(25, name.getBytes(Charsets.UTF_8));
        pStmt.addBatch();
        if (i % 1000 == 0) {
          pStmt.executeBatch();
        }
      }
      pStmt.executeBatch();
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

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

package io.cdap.plugin;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.batch.DatabasePluginTestBase;
import io.cdap.plugin.db.sink.ETLDBOutputFormat;
import io.cdap.plugin.db.source.DataDrivenETLDBInputFormat;
import io.cdap.plugin.jdbc.DatabaseAction;
import io.cdap.plugin.jdbc.DatabasePostAction;
import io.cdap.plugin.jdbc.DatabaseSink;
import io.cdap.plugin.jdbc.DatabaseSource;
import org.hsqldb.Server;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
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
import java.util.TimeZone;
import javax.sql.rowset.serial.SerialBlob;

/**
 * Database Plugin Tests setup.
 */
public abstract class GenericDatabasePluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final String CLOB_DATA =
    "this is a long string with line separators \n that can be used as \n a clob";
  protected static final long CURRENT_TS = System.currentTimeMillis();
  protected static final String JDBC_DRIVER_NAME = "hypersql";

  private static int startCount;
  private static HSQLDBServer hsqlDBServer;
  protected static Schema schema;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  static boolean tearDown = true;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("generic-database-plugin", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      DatabaseSource.class, DatabaseSink.class, DBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class, DatabaseAction.class, DatabasePostAction.class);

    // add hypersql 3rd party plugin
    PluginClass hypersql = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME, "hypersql jdbc driver",
                                           JDBCDriver.class.getName(), null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("hsql-jdbc", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(hypersql), JDBCDriver.class);


    String hsqlDBDir = temporaryFolder.newFolder("hsqldb").getAbsolutePath();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    hsqlDBServer = new HSQLDBServer(hsqlDBDir, "testdb");
    hsqlDBServer.start();
    try (Connection conn = hsqlDBServer.getConnection()) {
      createTestUser(conn);
      createTestTables(conn);
      prepareTestData(conn);
    }

    Schema nullableString = Schema.nullableOf(Schema.of(Schema.Type.STRING));
    Schema nullableBoolean = Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
    Schema nullableInt = Schema.nullableOf(Schema.of(Schema.Type.INT));
    Schema nullableLong = Schema.nullableOf(Schema.of(Schema.Type.LONG));
    Schema nullableFloat = Schema.nullableOf(Schema.of(Schema.Type.FLOAT));
    Schema nullableDouble = Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
    Schema nullableBytes = Schema.nullableOf(Schema.of(Schema.Type.BYTES));
    schema = Schema.recordOf("student",
                             Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
                             Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
                             Schema.Field.of("SCORE", nullableDouble),
                             Schema.Field.of("GRADUATED", nullableBoolean),
                             Schema.Field.of("TINY", nullableInt),
                             Schema.Field.of("SMALL", nullableInt),
                             Schema.Field.of("BIG", nullableLong),
                             Schema.Field.of("FLOAT_COL", nullableFloat),
                             Schema.Field.of("REAL_COL", nullableFloat),
                             Schema.Field.of("NUMERIC_COL", Schema.decimalOf(PRECISION, SCALE)),
                             Schema.Field.of("DECIMAL_COL", Schema.decimalOf(PRECISION, SCALE)),
                             Schema.Field.of("BIT_COL", nullableBoolean),
                             Schema.Field.of("DATE_COL", nullableLong),
                             Schema.Field.of("TIME_COL", nullableLong),
                             Schema.Field.of("TIMESTAMP_COL", nullableLong),
                             Schema.Field.of("BINARY_COL", nullableBytes),
                             Schema.Field.of("LONGVARBINARY_COL", nullableBytes),
                             Schema.Field.of("BLOB_COL", nullableBytes),
                             Schema.Field.of("CLOB_COL", nullableString),
                             Schema.Field.of("CHAR_COL", nullableString),
                             Schema.Field.of("LONGVARCHAR_COL", nullableString),
                             Schema.Field.of("VARBINARY_COL", nullableBytes));
  }


  private static void createTestUser(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE USER \"emptyPwdUser\" PASSWORD '' ADMIN");
    }
  }

  private static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // note that the tables need quotation marks around them; otherwise, hsql creates them in upper case
      stmt.execute("CREATE TABLE \"my_table\"" +
                     "(" +
                     "ID INT NOT NULL, " +
                     "NAME VARCHAR(40) NOT NULL, " +
                     "SCORE DOUBLE, " +
                     "GRADUATED BOOLEAN, " +
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
                     "TIME_COL TIME, " +
                     "TIMESTAMP_COL TIMESTAMP, " +
                     "BINARY_COL Binary(100)," +
                     "LONGVARBINARY_COL LONGVARBINARY(100)," +
                     "BLOB_COL BLOB(100), " +
                     "CLOB_COL CLOB(100)," +
                     "CHAR_COL CHAR(100)," +
                     "LONGVARCHAR_COL LONGVARCHAR," +
                     "VARBINARY_COL VARBINARY(20)" +
                     ")");
      stmt.execute("CREATE TABLE \"MY_DEST_TABLE\" AS (" +
                     "SELECT * FROM \"my_table\") WITH DATA");
      stmt.execute("CREATE TABLE \"your_table\" AS (" +
                     "SELECT * FROM \"my_table\") WITH DATA");
    }
  }

  private static void prepareTestData(Connection conn) throws SQLException {
    try (
      PreparedStatement pStmt1 =
        conn.prepareStatement("INSERT INTO \"my_table\" " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      PreparedStatement pStmt2 =
        conn.prepareStatement("INSERT INTO \"your_table\" " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
      // insert the same data into both tables: my_table and your_table
      final PreparedStatement[] preparedStatements = {pStmt1, pStmt2};
      for (PreparedStatement pStmt : preparedStatements) {
        for (int i = 1; i <= 5; i++) {
          String name = "user" + i;
          pStmt.setInt(1, i);
          pStmt.setString(2, name);
          pStmt.setDouble(3, 123.45 + i);
          pStmt.setBoolean(4, (i % 2 == 0));
          pStmt.setString(5, "random" + i);
          pStmt.setShort(6, (short) i);
          pStmt.setShort(7, (short) i);
          pStmt.setLong(8, (long) i);
          pStmt.setFloat(9, (float) 123.45 + i);
          pStmt.setFloat(10, (float) 123.45 + i);
          pStmt.setBigDecimal(11, new BigDecimal(123.45).add(new BigDecimal(i)));
          if ((i % 2 == 0)) {
            pStmt.setNull(12, Types.DECIMAL);
          } else {
            pStmt.setBigDecimal(12, new BigDecimal(123.45).add(new BigDecimal(i)));
          }
          pStmt.setBoolean(13, (i % 2 == 1));
          pStmt.setDate(14, new Date(CURRENT_TS));
          pStmt.setTime(15, new Time(CURRENT_TS));
          pStmt.setTimestamp(16, new Timestamp(CURRENT_TS));
          pStmt.setBytes(17, name.getBytes(Charsets.UTF_8));
          pStmt.setBytes(18, name.getBytes(Charsets.UTF_8));
          pStmt.setBlob(19, new SerialBlob(name.getBytes(Charsets.UTF_8)));
          pStmt.setClob(20, new InputStreamReader(new ByteArrayInputStream(CLOB_DATA.getBytes(Charsets.UTF_8))));
          pStmt.setString(21, "char" + i);
          pStmt.setString(22, "longvarchar" + i);
          pStmt.setBytes(23, name.getBytes(Charsets.UTF_8));
          pStmt.executeUpdate();
        }
      }
    }
  }

  @AfterClass
  public static void tearDownDB() throws SQLException {
    if (!tearDown) {
      return;
    }

    try (Connection conn = hsqlDBServer.getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE \"my_table\"");
      stmt.execute("DROP TABLE \"your_table\"");
      stmt.execute("DROP USER \"emptyPwdUser\"");
      stmt.execute("DROP TABLE \"MY_DEST_TABLE\"");
    }
    hsqlDBServer.stop();
  }

  protected String getConnectionURL() {
    return hsqlDBServer == null ? null : hsqlDBServer.getConnectionUrl();
  }

  protected String getDatabase() {
    return hsqlDBServer == null ? null : hsqlDBServer.getDatabase();
  }

  protected Connection getConnection() {
    return hsqlDBServer == null ? null : hsqlDBServer.getConnection();
  }

  private static class HSQLDBServer {
    private final String locationUrl;
    private final String database;
    private final String connectionUrl;
    private final Server server;
    private final String hsqlDBDriver = "org.hsqldb.jdbcDriver";

    private HSQLDBServer(String location, String database) {
      this.locationUrl = String.format("%s/%s", location, database);
      this.database = database;
      this.connectionUrl = String.format("jdbc:hsqldb:hsql://localhost/%s", database);
      this.server = new Server();
    }

    void start() {
      server.setDatabasePath(0, locationUrl);
      server.setDatabaseName(0, database);
      server.start();
    }

    void stop() {
      server.stop();
    }

    Connection getConnection() {
      try {
        Class.forName(hsqlDBDriver);
        return DriverManager.getConnection(connectionUrl);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String getConnectionUrl() {
      return this.connectionUrl;
    }

    String getDatabase() {
      return this.database;
    }
  }
}

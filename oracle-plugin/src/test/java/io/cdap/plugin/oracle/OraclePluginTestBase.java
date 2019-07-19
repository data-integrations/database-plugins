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

package io.cdap.plugin.oracle;

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
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

public class OraclePluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();
  private static final String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";

  protected static final String JDBC_DRIVER_NAME = "oracle";

  protected static final String MY_TABLE = "my_table";
  protected static final String MY_DEST_TABLE = "MY_DEST_TABLE";
  protected static final String YOUR_TABLE = "your_table";
  protected static final String MY_TABLE_FOR_LONG = "my_table_long";
  protected static final String MY_DEST_TABLE_FOR_LONG = "MY_DEST_LONG";

  protected static String connectionUrl;
  protected static final int YEAR;
  protected static final int DEFAULT_PRECISION = 38;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  protected static final ZoneId UTC = ZoneId.ofOffset("UTC", ZoneOffset.UTC);

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
    .put(ConnectionConfig.HOST, System.getProperty("oracle.host"))
    .put(ConnectionConfig.PORT, System.getProperty("oracle.port"))
    .put(ConnectionConfig.DATABASE, System.getProperty("oracle.database"))
    .put(ConnectionConfig.USER, System.getProperty("oracle.username"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("oracle.password"))
    .put(OracleConstants.CONNECTION_TYPE, System.getProperty("oracle.connectionType"))
    .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
    .put(OracleConstants.DEFAULT_BATCH_VALUE, "10")
    .build();

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact(JDBC_DRIVER_NAME, "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      OracleSource.class, OracleSink.class, DBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class, OraclePostAction.class, OracleAction.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    // add oracle 3rd party plugin
    PluginClass oracleDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                                               "oracle driver class", driverClass.getName(),
                                               null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("oracle-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(oracleDriver), driverClass);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(BASE_PROPS.get("oracle.connectionType"))) {
      connectionUrl = String.format(OracleConstants.ORACLE_CONNECTION_SERVICE_NAME_STRING_FORMAT,
                                    BASE_PROPS.get(ConnectionConfig.HOST),
                                    BASE_PROPS.get(ConnectionConfig.PORT),
                                    BASE_PROPS.get(ConnectionConfig.DATABASE)
      );
    } else {
      connectionUrl = String.format(OracleConstants.ORACLE_CONNECTION_STRING_FORMAT,
                                    BASE_PROPS.get(ConnectionConfig.HOST),
                                    BASE_PROPS.get(ConnectionConfig.PORT),
                                    BASE_PROPS.get(ConnectionConfig.DATABASE)
      );
    }

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

      String createTableFormat = "CREATE TABLE %s (" +
                     "  ID INT NOT NULL, " +
                     "  CHAR_COL CHAR(10)," +
                     "  NCHAR_COL NCHAR(10)," +
                     "  CHARACTER_COL CHARACTER(10)," +
                     "  VARCHAR_COL VARCHAR(10)," +
                     "  VARCHAR2_COL VARCHAR2(10)," +
                     "  NVARCHAR2_COL NVARCHAR2(10)," +
                     "  INT_COL INT," + // synonym for NUMBER(38,0)
                     "  INTEGER_COL INTEGER," + // synonym for NUMBER(38,0)
                     "  DEC_COL DEC," +
                     "  DECIMAL_COL DECIMAL(" + PRECISION + "," + SCALE + ")," +
                     "  NUMBER_COL NUMBER(" + PRECISION + "," + SCALE + ")," +
                     "  NUMERIC_COL NUMERIC(" + PRECISION + "," + SCALE + ")," +
                     "  SMALLINT_COL SMALLINT," + // synonym for NUMBER(38,0)
                     "  REAL_COL REAL," + // FLOAT(63), value is represented internally as NUMBER.
                     "  DATE_COL DATE," +
                     "  TIMESTAMP_COL TIMESTAMP," +
                     "  TIMESTAMPTZ_COL TIMESTAMP WITH TIME ZONE," +
                     "  TIMESTAMPLTZ_COL TIMESTAMP WITH LOCAL TIME ZONE," +
                     "  INTERVAL_YEAR_TO_MONTH_COL INTERVAL YEAR(3) TO MONTH," +
                     "  INTERVAL_DAY_TO_SECOND_COL INTERVAL DAY(2) TO SECOND," +
                     "  RAW_COL RAW(16)," +
                     "  CLOB_COL CLOB," +
                     "  BLOB_COL BLOB," +
                     "  NCLOB_COL NCLOB," +
                     "  FLOAT_COL FLOAT," + // FLOAT(126), value is represented internally as NUMBER.
                     "  BINARY_FLOAT_COL BINARY_FLOAT," +
                     "  BINARY_DOUBLE_COL BINARY_DOUBLE," +
                     "  LONG_RAW_COL LONG RAW," +
                     "  ROWID_COL ROWID," +
                     "  UROWID_COL UROWID," +
                     "  BFILE_COL BFILE" +
                     ")";

      String createTableWithLongFormat = "CREATE TABLE %s (" +
                     "  ID INT NOT NULL, " +
                     "  SMALLINT_COL SMALLINT," + // synonym for NUMBER(38,0)
                     "  VARCHAR_COL VARCHAR(10)," +
                     "  LONG_COL LONG" +
                     ")";

      stmt.execute(String.format(createTableFormat, MY_TABLE));
      stmt.execute(String.format(createTableFormat, MY_DEST_TABLE));
      stmt.execute(String.format(createTableFormat, YOUR_TABLE));

      stmt.execute(String.format(createTableWithLongFormat, MY_TABLE_FOR_LONG));
      stmt.execute(String.format(createTableWithLongFormat, MY_DEST_TABLE_FOR_LONG));
    }
  }

  protected static void prepareTestData(Connection conn) throws Exception {
    // specify column names to exclude BFILE_COL, which will be set via BFILENAME function
    String insertTableFormat = "INSERT INTO %s(ID, CHAR_COL, NCHAR_COL, CHARACTER_COL, VARCHAR_COL, VARCHAR2_COL, " +
      "NVARCHAR2_COL, INT_COL, INTEGER_COL, DEC_COL, DECIMAL_COL, NUMBER_COL, NUMERIC_COL, SMALLINT_COL, REAL_COL, " +
      "DATE_COL, TIMESTAMP_COL, TIMESTAMPTZ_COL, TIMESTAMPLTZ_COL, INTERVAL_YEAR_TO_MONTH_COL, " +
      "INTERVAL_DAY_TO_SECOND_COL, RAW_COL, CLOB_COL, BLOB_COL, NCLOB_COL, FLOAT_COL, BINARY_FLOAT_COL, " +
      "BINARY_DOUBLE_COL, LONG_RAW_COL, ROWID_COL, UROWID_COL) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
      "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (
      Statement stmt = conn.createStatement();
      PreparedStatement pStmt1 =
        conn.prepareStatement(String.format(insertTableFormat, MY_TABLE));
      PreparedStatement pStmt2 =
        conn.prepareStatement(String.format(insertTableFormat, YOUR_TABLE));

      PreparedStatement pStmt3 =
        conn.prepareStatement("INSERT INTO my_table_long VALUES(?, ?, ?, ?)")) {

      stmt.execute("insert into dbActionTest values (1, '1970-01-01')");
      stmt.execute("insert into postActionTest values (1, '1970-01-01')");

      populateData(pStmt1, MY_TABLE);
      populateData(pStmt2, YOUR_TABLE);
      populateDataLong(pStmt3);
    }
  }

  private static void populateData(PreparedStatement pStmt, String tableName) throws Exception {
    // insert the same data into both tables: my_table and your_table
    for (int i = 1; i <= 5; i++) {
      Clob clob = null;
      NClob nClob = null;
      try {
        String name = "user" + i;
        pStmt.setInt(1, i);
        pStmt.setString(2, name);
        pStmt.setString(3, name);
        pStmt.setString(4, name);
        pStmt.setString(5, name);
        pStmt.setString(6, name);
        pStmt.setString(7, name);
        pStmt.setInt(8, 42 + i);
        pStmt.setInt(9, 24 + i);
        pStmt.setBigDecimal(10, new BigDecimal(54.56 + i));
        pStmt.setBigDecimal(11, new BigDecimal(54.65 + i));
        pStmt.setBigDecimal(12, new BigDecimal(32.65 + i));
        pStmt.setBigDecimal(13, new BigDecimal(23.65 + i));
        pStmt.setInt(14, i);
        pStmt.setFloat(15, (float) 14.45 + i);
        pStmt.setDate(16, new Date(CURRENT_TS));
        pStmt.setTimestamp(17, new Timestamp(CURRENT_TS));
        pStmt.setObject(18,
                        createTimestampWithTimezone(pStmt.getConnection(), "2019-07-15 15:57:46.65 GMT"));
        pStmt.setTimestamp(19, new Timestamp(CURRENT_TS));
        pStmt.setString(20, "300-5");
        pStmt.setString(21, "23 3:02:10");
        pStmt.setBytes(22, name.getBytes());

        clob = pStmt.getConnection().createClob();
        clob.setString(1, name);
        pStmt.setClob(23, clob);

        pStmt.setBytes(24, name.getBytes());

        nClob = pStmt.getConnection().createNClob();
        nClob.setString(1, name);
        pStmt.setNClob(25, nClob);

        pStmt.setFloat(26, (float) 123.45 + i);
        pStmt.setFloat(27, (float) 123.45 + i);
        pStmt.setDouble(28, 123.45 + i);

        pStmt.setBytes(29, name.getBytes());
        pStmt.setString(30, "AAAUEVAAFAAAAR/AA" + i);
        pStmt.setString(31, "AAAUEVAAFAAAAR/AA" + i);

        pStmt.executeUpdate();

        // Create BFILE locator (link) to an nonexistent external binary file (file stored outside of the database).
        // It's not possible to create an operating system file that a BFILE would refer to, those are created only
        // externally. But it's possible to create a locator (link) to an non-existing file.
        Statement stmt = pStmt.getConnection().createStatement();
        String setBfileStmt = String.format("UPDATE %s SET BFILE_COL=BFILENAME('ORACLE_HOME', 'nonexistent.txt') " +
                                              "WHERE ID=%d", tableName, i);
        stmt.execute(setBfileStmt);
      } finally {
        if (Objects.nonNull(clob)) {
          clob.free();
        }

        if (Objects.nonNull(nClob)) {
          nClob.free();
        }
      }
    }
  }

  protected static byte[] getBfileBytes(Object bfile) throws Exception {
    Class<?> bfileClass = Class.forName("oracle.sql.BFILE");
    return (byte[]) bfileClass.getMethod("getBytes").invoke(bfile);
  }

  private static Object createTimestampWithTimezone(Connection connection, String timestampString) throws Exception {
    Class<?> timestampTZClass = Class.forName("oracle.sql.TIMESTAMPTZ");
    return timestampTZClass.getConstructor(Connection.class, String.class).newInstance(connection, timestampString);
  }

  private static void populateDataLong(PreparedStatement pStmt) throws SQLException {
    for (int i = 1; i <= 5; i++) {
      String name = "user" + i;
      pStmt.setInt(1, i);
      pStmt.setInt(2, i);
      pStmt.setString(3, name);
      pStmt.setObject(4, name);

      pStmt.executeUpdate();
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

    String dropTableFormat = "DROP TABLE %s";

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute(String.format(dropTableFormat, MY_TABLE));
      stmt.execute(String.format(dropTableFormat, MY_TABLE_FOR_LONG));
      stmt.execute(String.format(dropTableFormat, YOUR_TABLE));
      stmt.execute(String.format(dropTableFormat, "postActionTest"));
      stmt.execute(String.format(dropTableFormat, "dbActionTest"));
      stmt.execute(String.format(dropTableFormat, MY_DEST_TABLE));
      stmt.execute(String.format(dropTableFormat, MY_DEST_TABLE_FOR_LONG));
    }
  }
}

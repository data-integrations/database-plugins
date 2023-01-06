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
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.batch.DatabasePluginTestBase;
import io.cdap.plugin.db.batch.sink.ETLDBOutputFormat;
import io.cdap.plugin.db.batch.source.DataDrivenETLDBInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public abstract class SqlServerPluginTestBase extends DatabasePluginTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerPluginTestBase.class);
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();
  protected static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  protected static final String JDBC_DRIVER_NAME = "sqlserver42";

  protected static final List<ByteBuffer> TIMESTAMP_VALUES = new ArrayList<>();
  protected static final List<ByteBuffer> GEOMETRY_VALUES = new ArrayList<>();
  protected static final List<ByteBuffer> GEOGRAPHY_VALUES = new ArrayList<>();
  protected static final Map<String, String> BASE_PROPS = new HashMap<>();

  protected static String connectionUrl;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  protected static final int MONEY_PRECISION = 19;
  protected static final int MONEY_SCALE = 4;
  protected static final int SMALL_MONEY_PRECISION = 10;
  protected static final int SMALL_MONEY_SCALE = 4;
  protected static final LocalTime TIME_MICROS = LocalTime.of(16, 17, 18, 123456000);
  protected static final ZoneId UTC = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    getProperties();

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

  private static void getProperties() {
    BASE_PROPS.put(ConnectionConfig.HOST, getPropertyOrSkip("mssql.host"));
    BASE_PROPS.put(ConnectionConfig.PORT, getPropertyOrSkip("mssql.port"));
    BASE_PROPS.put(ConnectionConfig.DATABASE, getPropertyOrSkip("mssql.database"));
    BASE_PROPS.put(ConnectionConfig.USER, getPropertyOrSkip("mssql.username"));
    BASE_PROPS.put(ConnectionConfig.PASSWORD, getPropertyOrSkip("mssql.password"));
    BASE_PROPS.put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME);
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // create UDT which is actually alias for varchar(11)
      stmt.execute("CREATE TYPE SSN FROM varchar(11) NOT NULL");
      // create UDT which is actually alias for varchar(11)
      stmt.execute("CREATE TYPE BIG_UDT FROM BIGINT NOT NULL");
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
                     "DATETIME_COL DATETIME, " +
                     "DATETIME2_COL DATETIME2, " +
                     "DATETIMEOFFSET_COL DATETIMEOFFSET, " +
                     "SMALLDATETIME_COL SMALLDATETIME, " +
                     "TIMESTAMP_COL TIMESTAMP, " +
                     "TIME_COL TIME, " +
                     "IMAGE_COL IMAGE, " +
                     "MONEY_COL MONEY, " +
                     "SMALLMONEY_COL SMALLMONEY, " +
                     "NCHAR_COL NCHAR(100), " +
                     "NTEXT_COL NTEXT, " +
                     "NVARCHAR_COL NVARCHAR(100), " +
                     "NVARCHAR_MAX_COL NVARCHAR(max), " +
                     "VARCHAR_MAX_COL VARCHAR(max), " +
                     "TEXT_COL TEXT, " +
                     "CHAR_COL CHAR(100), " +
                     "BINARY_COL BINARY(5), " +
                     "VARBINARY_COL VARBINARY(20), " +
                     "VARBINARY_MAX_COL VARBINARY(MAX), " +
                     "UNIQUEIDENTIFIER_COL UNIQUEIDENTIFIER, " +
                     "XML_COL XML, " +
                     "SQL_VARIANT_COL SQL_VARIANT, " +
                     "GEOMETRY_COL GEOMETRY, " +
                     "GEOGRAPHY_COL GEOGRAPHY, " +
                     "GEOMETRY_WKT_COL GEOMETRY, " +
                     "GEOGRAPHY_WKT_COL GEOGRAPHY," +
                     "UDT_COL SSN," +
                     "BIG_UDT_COL BIG_UDT" +
                     ")");
      stmt.execute("SELECT * INTO MY_DEST_TABLE FROM my_table");
      stmt.execute("SELECT * INTO your_table FROM my_table");
    }
  }

  protected static void prepareTestData(Connection conn) throws SQLException {
    // specify column names to exclude TIMESTAMP_COL which is automatically generated and can not be set
    String insertIntoFormat = "INSERT INTO %s(ID, NAME, NOT_IMPORTED, TINY, SMALL, BIG, FLOAT_COL, REAL_COL, " +
      "NUMERIC_COL, DECIMAL_COL, BIT_COL, DATE_COL, DATETIME_COL, DATETIME2_COL, DATETIMEOFFSET_COL, " +
      "SMALLDATETIME_COL, TIME_COL, IMAGE_COL, MONEY_COL, SMALLMONEY_COL, NCHAR_COL, NTEXT_COL, NVARCHAR_COL, " +
      "NVARCHAR_MAX_COL, VARCHAR_MAX_COL, TEXT_COL, CHAR_COL, BINARY_COL, VARBINARY_COL, VARBINARY_MAX_COL, " +
      "UNIQUEIDENTIFIER_COL, XML_COL, SQL_VARIANT_COL, GEOMETRY_COL, GEOGRAPHY_COL, GEOMETRY_WKT_COL, " +
      "GEOGRAPHY_WKT_COL, UDT_COL, BIG_UDT_COL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
      "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (
      Statement stmt = conn.createStatement();
      PreparedStatement pStmt1 = conn.prepareStatement(String.format(insertIntoFormat, "my_table"));
      PreparedStatement pStmt2 = conn.prepareStatement(String.format(insertIntoFormat, "your_table"))) {
      stmt.execute("insert into dbActionTest values (1, '1970-01-01')");
      stmt.execute("insert into postActionTest values (1, '1970-01-01')");
      populateData(pStmt1, pStmt2);
    }
    try (Statement stmt = conn.createStatement(); ResultSet resultSet =
      stmt.executeQuery("SELECT ID, TIMESTAMP_COL, GEOMETRY_COL, GEOGRAPHY_COL FROM my_table ORDER BY ID")) {
      while (resultSet.next()) {
        TIMESTAMP_VALUES.add(ByteBuffer.wrap(resultSet.getBytes("TIMESTAMP_COL")));
        GEOMETRY_VALUES.add(ByteBuffer.wrap(resultSet.getBytes("GEOMETRY_COL")));
        GEOGRAPHY_VALUES.add(ByteBuffer.wrap(resultSet.getBytes("GEOGRAPHY_COL")));
      }
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
        pStmt.setString(15, "2019-06-24 16:19:15.8010000 +03:00");
        pStmt.setTimestamp(16, new Timestamp(CURRENT_TS));
        pStmt.setString(17, TIME_MICROS.toString());
        pStmt.setBytes(18, name.getBytes());
        pStmt.setBigDecimal(19, new BigDecimal(123.45f + i));
        pStmt.setFloat(20, 123.45f + (short) i);
        pStmt.setString(21, name);
        pStmt.setString(22, name);
        pStmt.setString(23, name);
        pStmt.setString(24, name);
        pStmt.setString(25, name);
        pStmt.setString(26, name);
        pStmt.setString(27, "char" + i);
        pStmt.setBytes(28, name.getBytes(Charsets.UTF_8));
        pStmt.setBytes(29, name.getBytes(Charsets.UTF_8));
        pStmt.setBytes(30, name.getBytes(Charsets.UTF_8));
        pStmt.setString(31, "0E984725-C51C-4BF4-9960-E1C80E27ABA" + i);
        pStmt.setString(32, "<root><child/></root>");
        pStmt.setString(33, name);
        pStmt.setString(34, "POINT(3 40 5 6)");
        pStmt.setString(35, "POINT(3 40 5 6)");
        pStmt.setString(36, "POINT(3 40 5 6)");
        pStmt.setString(37, "POINT(3 40 5 6)");
        pStmt.setString(38, "12345678910");
        pStmt.setLong(39, 15417543010L);
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
  public static void tearDownDB() {
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      executeCleanup(Arrays.<Cleanup>asList(() -> stmt.execute("DROP TABLE my_table"),
                                            () -> stmt.execute("DROP TABLE your_table"),
                                            () -> stmt.execute("DROP TABLE postActionTest"),
                                            () -> stmt.execute("DROP TABLE dbActionTest"),
                                            () -> stmt.execute("DROP TABLE MY_DEST_TABLE"),
                                            () -> stmt.execute("DROP TYPE SSN"),
                                            () -> stmt.execute("DROP TYPE BIG_UDT")), LOGGER);

    } catch (Exception e) {
      LOGGER.warn("Fail to tear down.", e);
    }
  }
}

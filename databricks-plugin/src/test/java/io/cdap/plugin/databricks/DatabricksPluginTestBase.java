/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.databricks;

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
import io.cdap.plugin.db.sink.ETLDBOutputFormat;
import io.cdap.plugin.db.source.DataDrivenETLDBInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Base test class for Databricks plugins.
 */
public abstract class DatabricksPluginTestBase extends DatabasePluginTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksPluginTestBase.class);
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();

  protected static final String DRIVER_CLASS = "com.databricks.client.jdbc.Driver";
  protected static final String JDBC_DRIVER_NAME = "databricks";
  protected static final Map<String, String> BASE_PROPS = new HashMap<>();

  protected static String connectionUrl;
  protected static int year;
  protected static final int PRECISION = 10;
  protected static final int SCALE = 6;
  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    getProperties();

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date(CURRENT_TS));
    year = calendar.get(Calendar.YEAR);

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact(JDBC_DRIVER_NAME, "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      DatabricksSource.class, DatabricksDBRecord.class, DBRecord.class,
                      ETLDBOutputFormat.class, DataDrivenETLDBInputFormat.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    PluginClass databricksDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME,
                                                   "databricks driver class", driverClass.getName(),
                                                   null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("databricks-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(databricksDriver), driverClass);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    connectionUrl = String.format(DatabricksConstants.DATABRICKS_DB_CONNECTION_STRING_FORMAT,
                                  BASE_PROPS.get(ConnectionConfig.HOST),
                                  Integer.parseInt(BASE_PROPS.get(ConnectionConfig.PORT)),
                                  BASE_PROPS.get(ConnectionConfig.DATABASE),
                                  BASE_PROPS.get(DatabricksConnectorConfig.HTTP_PATH));
    Connection conn = createConnection();
    createTestTables(conn);
    prepareTestData(conn);
  }

  private static void getProperties() {
    BASE_PROPS.put(ConnectionConfig.HOST, getPropertyOrSkip("databricks.host"));
    BASE_PROPS.put(ConnectionConfig.PORT, getPropertyOrSkip("databricks.port"));
    BASE_PROPS.put(ConnectionConfig.DATABASE, getPropertyOrSkip("databricks.database"));
    BASE_PROPS.put(DatabricksConnectorConfig.HTTP_PATH, getPropertyOrSkip("databricks.httpPath"));
    BASE_PROPS.put(ConnectionConfig.USER, getPropertyOrSkip("databricks.username"));
    BASE_PROPS.put(ConnectionConfig.PASSWORD, getPropertyOrSkip("databricks.password"));
    BASE_PROPS.put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME);
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE IF NOT EXISTS my_table (" +
                     "ID INT NOT NULL, " +
                     "NAME VARCHAR(40) NOT NULL, " +
                     "SCORE FLOAT, " +
                     "GRADUATED BOOLEAN, " +
                     "NOT_IMPORTED VARCHAR(30), " +
                     "SMALLINT_COL SMALLINT, " +
                     "BIG BIGINT, " +
                     "NUMERIC_COL DECIMAL(" + PRECISION + "," + SCALE + "), " +
                     "DECIMAL_COL DECIMAL(" + PRECISION + "," + SCALE + "), " +
                     "DOUBLE_PREC_COL DOUBLE, " +
                     "DATE_COL DATE, " +
                     "TIMESTAMP_COL TIMESTAMP, " +
                     "TIMESTAMP_NTZ_COL TIMESTAMP_NTZ, " +
                     "TEXT_COL STRING, " +
                     "CHAR_COL CHAR(100), " +
                     "BYTEA_COL BINARY" +
                     ")");
      stmt.execute("CREATE TABLE IF NOT EXISTS your_table AS SELECT * FROM my_table");
    }
  }

  protected static void prepareTestData(Connection conn) throws SQLException {
    try (
      PreparedStatement pStmt1 =
        conn.prepareStatement("INSERT INTO my_table " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      PreparedStatement pStmt2 =
        conn.prepareStatement("INSERT INTO your_table " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
      populateData(pStmt1, pStmt2);
    }
  }

  private static void populateData(PreparedStatement... stmts) throws SQLException {
    for (PreparedStatement pStmt : stmts) {
      for (int i = 1; i <= 5; i++) {
        String name = "user" + i;
        pStmt.setInt(1, i);
        pStmt.setString(2, name);
        pStmt.setFloat(3, 123.45f + i);
        pStmt.setBoolean(4, (i % 2 == 0));
        pStmt.setString(5, "random" + i);
        pStmt.setShort(6, (short) i);
        pStmt.setLong(7, (long) i);
        pStmt.setBigDecimal(8, new BigDecimal("123.45").add(new BigDecimal(i)));
        pStmt.setBigDecimal(9, new BigDecimal("123.45").add(new BigDecimal(i)));
        pStmt.setDouble(10, 123.45 + i);
        pStmt.setDate(11, new Date(CURRENT_TS));
        pStmt.setTimestamp(12, new Timestamp(CURRENT_TS));
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
      Class.forName(DRIVER_CLASS);
      return DriverManager.getConnection(connectionUrl, BASE_PROPS.get(ConnectionConfig.USER),
                                         BASE_PROPS.get(ConnectionConfig.PASSWORD));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @AfterClass
  public static void tearDownDB() {
    if (connectionUrl == null) {
      return;
    }
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      executeCleanup(Arrays.<Cleanup>asList(() -> stmt.execute("DROP TABLE IF EXISTS my_table"),
                                            () -> stmt.execute("DROP TABLE IF EXISTS your_table")), LOGGER);
    } catch (Exception e) {
      LOGGER.warn("Fail to tear down.", e);
    }
  }
}

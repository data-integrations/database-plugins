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

package io.cdap.plugin.db.connector;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.ReadOnlyConnection;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Unit tests for the security hardening in {@link AbstractDBSpecificConnector}: identifier quoting, table
 * validation before any query is executed, and read-only connections. No real database is needed, a mock JDBC
 * driver returning Mockito connections is registered with the {@link DriverManager}.
 */
public class AbstractDBSpecificConnectorTest {

  private static final String MOCK_URL = "jdbc:cdap-mock:db";
  /** Table component of the sample request path used in the original vulnerability report. */
  private static final String INJECTION_PAYLOAD =
    "pg_class\" LIMIT 0; UPDATE pg_catalog.pg_class SET relname = 'x'; SELECT * FROM \"pg_catalog\".\"pg_class";
  private static final MockDriver DRIVER = new MockDriver();

  private Connection connection;
  private DatabaseMetaData metaData;
  private ResultSet tables;
  private TestConnector connector;

  @BeforeClass
  public static void registerDriver() throws SQLException {
    DriverManager.registerDriver(DRIVER);
  }

  @AfterClass
  public static void deregisterDriver() throws SQLException {
    DriverManager.deregisterDriver(DRIVER);
  }

  @Before
  public void setUp() throws SQLException {
    connection = Mockito.mock(Connection.class);
    metaData = Mockito.mock(DatabaseMetaData.class);
    tables = Mockito.mock(ResultSet.class);
    Mockito.when(connection.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getTables(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(tables);
    DRIVER.connection = connection;
    connector = new TestConnector(new TestConfig());
  }

  @After
  public void tearDown() {
    DRIVER.connection = null;
  }

  @Test
  public void testGetTableNameQuotesAndEscapesEveryIdentifier() {
    Assert.assertEquals("\"db\".\"schema\".\"table\"", connector.getTableName("db", "schema", "table"));
    Assert.assertEquals("\"db\".\"table\"", connector.getTableName("db", null, "table"));
    Assert.assertEquals("\"d\"\"b\".\"sch\"\"ema\".\"ta\"\"ble\"",
                        connector.getTableName("d\"b", "sch\"ema", "ta\"ble"));
  }

  @Test
  public void testInjectionPayloadStaysInsideTheIdentifier() throws IOException {
    String expectedTable = "\"db\".\"pg_catalog\".\"pg_class\"\" LIMIT 0; UPDATE pg_catalog.pg_class "
      + "SET relname = 'x'; SELECT * FROM \"\"pg_catalog\"\".\"\"pg_class\"";
    Assert.assertEquals(expectedTable, connector.getTableName("db", "pg_catalog", INJECTION_PAYLOAD));
    Assert.assertEquals("SELECT * FROM " + expectedTable,
                        connector.getTableQuery("db", "pg_catalog", INJECTION_PAYLOAD));
    Assert.assertEquals("SELECT * FROM " + expectedTable + " LIMIT 10",
                        connector.getTableQuery("db", "pg_catalog", INJECTION_PAYLOAD, 10, null, null, "s"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTableNameRejectsNulCharacters() {
    connector.getTableName("db", "schema", "ta\0ble");
  }

  @Test
  public void testValidateTablePassesWhenTableExists() throws SQLException {
    Mockito.when(tables.next()).thenReturn(true);

    connector.validateTable(connection, "db", "schema", "table");

    Mockito.verify(metaData).getTables("db", "schema", "table", null);
    Mockito.verify(tables).close();
  }

  @Test
  public void testValidateTableFailsWhenTableDoesNotExist() throws SQLException {
    Mockito.when(tables.next()).thenReturn(false);

    try {
      connector.validateTable(connection, "db", "schema", INJECTION_PAYLOAD);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not exist in schema 'schema'"));
    }
    Mockito.verify(metaData).getTables("db", "schema", INJECTION_PAYLOAD, null);
    Mockito.verify(tables).close();
    // no statement may ever be created, let alone executed, for an unknown table
    Mockito.verify(connection, Mockito.never()).createStatement();
  }

  @Test
  public void testGetTableSchemaValidatesTableBeforeExecutingAnyQuery() throws SQLException {
    Mockito.when(tables.next()).thenReturn(false);

    try {
      connector.getTableSchema(connection, "db", "schema", INJECTION_PAYLOAD);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    Mockito.verify(connection, Mockito.never()).createStatement();
    Mockito.verify(connection, Mockito.never()).prepareStatement(Mockito.anyString());
  }

  @Test
  public void testGetTableSchemaUsesTheGivenConnectionAndQuotedQuery() throws SQLException {
    Mockito.when(tables.next()).thenReturn(true);
    Statement statement = mockSingleIntColumnQuery(connection);
    // a different connection is returned by the driver; it must not be used by getTableSchema
    Connection otherConnection = Mockito.mock(Connection.class);
    DRIVER.connection = otherConnection;

    Schema schema = connector.getTableSchema(connection, "db", "sch\"ema", "ta\"ble");

    Assert.assertEquals(1, schema.getFields().size());
    Assert.assertEquals("id", schema.getFields().get(0).getName());
    InOrder inOrder = Mockito.inOrder(metaData, statement);
    inOrder.verify(metaData).getTables("db", "sch\"ema", "ta\"ble", null);
    inOrder.verify(statement).executeQuery("SELECT * FROM \"db\".\"sch\"\"ema\".\"ta\"\"ble\"");
    Mockito.verify(statement).setMaxRows(1);
    Mockito.verifyZeroInteractions(otherConnection);
  }

  @Test
  public void testGetConnectionReturnsReadOnlyNonAutoCommitConnection() throws SQLException {
    Connection hardened = connector.getConnection(MOCK_URL, new Properties());

    Assert.assertTrue(hardened instanceof ReadOnlyConnection);
    InOrder inOrder = Mockito.inOrder(connection);
    inOrder.verify(connection).setAutoCommit(false);
    inOrder.verify(connection).setReadOnly(true);

    hardened.close();
    inOrder.verify(connection).rollback();
    inOrder.verify(connection).close();
  }

  @Test
  public void testGetConnectionFailsClosedWhenReadOnlyCannotBeEnabled() throws SQLException {
    Mockito.doThrow(new SQLException("not supported")).when(connection).setReadOnly(true);

    try {
      connector.getConnection(MOCK_URL, new Properties());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("read-only"));
    }
    Mockito.verify(connection, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testSampleFailsBeforeAnyQueryWhenTableDoesNotExist() throws SQLException {
    Mockito.when(tables.next()).thenReturn(false);
    SampleRequest request = SampleRequest.builder(10).setPath("db/pg_catalog/" + INJECTION_PAYLOAD).build();

    try {
      connector.getInputFormatProvider(Mockito.mock(ConnectorContext.class), request);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    } catch (IOException e) {
      throw new AssertionError("Expected IllegalArgumentException", e);
    }
    Mockito.verify(metaData).getTables("db", "pg_catalog", INJECTION_PAYLOAD, null);
    Mockito.verify(connection, Mockito.never()).createStatement();
    Mockito.verify(connection, Mockito.never()).prepareStatement(Mockito.anyString());
    // the connection opened for schema discovery is always closed
    Mockito.verify(connection).close();
  }

  @Test
  public void testSampleUsesQuotedQueryAndReadOnlyConnection() throws SQLException, IOException {
    Mockito.when(tables.next()).thenReturn(true);
    Statement statement = mockSingleIntColumnQuery(connection);
    SampleRequest request = SampleRequest.builder(10).setPath("db/sch\"ema/ta\"ble").build();

    InputFormatProvider provider = connector.getInputFormatProvider(Mockito.mock(ConnectorContext.class), request);

    String expectedQuery = "SELECT * FROM \"db\".\"sch\"\"ema\".\"ta\"\"ble\" LIMIT 10";
    Mockito.verify(statement).executeQuery(expectedQuery);
    Map<String, String> conf = provider.getInputFormatConfiguration();
    Assert.assertEquals(expectedQuery, conf.get(DBConfiguration.INPUT_QUERY));
    Assert.assertEquals("true", conf.get(ConnectionConfigAccessor.READ_ONLY));
    Assert.assertEquals("false", conf.get(ConnectionConfigAccessor.AUTO_COMMIT_ENABLED));
    Assert.assertNotNull(conf.get(ConnectionConfigAccessor.OVERRIDE_SCHEMA));
    // schema discovery ran on a hardened connection that was closed afterwards
    InOrder inOrder = Mockito.inOrder(connection);
    inOrder.verify(connection).setAutoCommit(false);
    inOrder.verify(connection).setReadOnly(true);
    inOrder.verify(connection).createStatement();
    inOrder.verify(connection).rollback();
    inOrder.verify(connection).close();
  }

  @Test
  public void testStratifiedSampleRejectsMissingStrataBeforeAnyQuery() throws SQLException {
    Mockito.when(tables.next()).thenReturn(true);
    SampleRequest request = SampleRequest.builder(10).setPath("db/schema/table")
      .setProperties(Collections.singletonMap("sampleType", "stratified")).build();

    try {
      connector.getInputFormatProvider(Mockito.mock(ConnectorContext.class), request);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("strata"));
    } catch (IOException e) {
      throw new AssertionError("Expected IllegalArgumentException", e);
    }
    Mockito.verify(connection, Mockito.never()).createStatement();
    Mockito.verify(connection).close();
  }

  /**
   * Makes the given connection answer any query with a single row result set that has one signed INTEGER column.
   */
  private static Statement mockSingleIntColumnQuery(Connection connection) throws SQLException {
    Statement statement = Mockito.mock(Statement.class);
    ResultSet rows = Mockito.mock(ResultSet.class);
    ResultSetMetaData rowsMetaData = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(rows);
    Mockito.when(rows.getMetaData()).thenReturn(rowsMetaData);
    Mockito.when(rowsMetaData.getColumnCount()).thenReturn(1);
    Mockito.when(rowsMetaData.getColumnName(1)).thenReturn("id");
    Mockito.when(rowsMetaData.getColumnType(1)).thenReturn(Types.INTEGER);
    Mockito.when(rowsMetaData.getColumnTypeName(1)).thenReturn("int4");
    Mockito.when(rowsMetaData.isNullable(1)).thenReturn(ResultSetMetaData.columnNoNulls);
    Mockito.when(rowsMetaData.isSigned(1)).thenReturn(true);
    return statement;
  }

  /**
   * Minimal connector configuration pointing at the mock driver.
   */
  private static final class TestConfig extends AbstractDBConnectorConfig {
    @Override
    public String getConnectionString() {
      return MOCK_URL;
    }
  }

  /**
   * Minimal concrete connector exposing the protected hooks under test.
   */
  private static final class TestConnector extends AbstractDBSpecificConnector<DBRecord> {
    TestConnector(AbstractDBConnectorConfig config) {
      super(config);
      // normally set by configure(); needed to build the input format configuration
      driverClass = MockDriver.class;
    }

    @Override
    public boolean supportSchema() {
      return true;
    }

    @Override
    protected Class<? extends DBWritable> getDBRecordType() {
      return DBRecord.class;
    }

    @Override
    public StructuredRecord transform(LongWritable longWritable, DBRecord record) {
      return record.getRecord();
    }
  }

  /**
   * JDBC driver returning the configured mock connection for {@link #MOCK_URL}.
   */
  public static final class MockDriver implements Driver {
    private volatile Connection connection;

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) {
        return null;
      }
      if (connection == null) {
        throw new SQLException("No mock connection configured.");
      }
      return connection;
    }

    @Override
    public boolean acceptsURL(String url) {
      return url != null && url.startsWith("jdbc:cdap-mock:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }
  }
}

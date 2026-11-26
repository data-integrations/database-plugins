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

package io.cdap.plugin.db.source;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.NoOpCommitConnection;
import io.cdap.plugin.db.RollbackOnCloseConnection;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

@RunWith(MockitoJUnitRunner.class)
public class DataDrivenETLDBInputFormatTest {

  private static final String STUB_URL = "jdbc:stubinput://localhost:1234/db";

  @Mock
  private JobContext mockJobContext;
  @Mock
  private DBConfiguration mockDbConfiguration;

  private DataDrivenETLDBInputFormat inputFormat;

  @Before
  public void setUp() throws SQLException {
    deregisterStubDrivers();
    StubJdbcDriver.nextConnection = null;
    inputFormat = Mockito.spy(new DataDrivenETLDBInputFormat());
    Mockito.doReturn(mockDbConfiguration).when(inputFormat).getDBConf();
    Mockito.doReturn("id").when(mockDbConfiguration).getInputOrderBy();
  }

  @After
  public void tearDown() throws SQLException {
    StubJdbcDriver.nextConnection = null;
    deregisterStubDrivers();
  }

  @Test
  public void testGetSplitsAddsNullSplit() throws IOException {
    DataDrivenDBInputFormat.DataDrivenDBInputSplit existingSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("id >= 0", "id < 100");
    List<InputSplit> initialSplits = ImmutableList.of(existingSplit);
    Mockito.doReturn(initialSplits).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);

    Assert.assertEquals("A new split for NULLs should be added", 2, finalSplits.size());

    DataDrivenDBInputFormat.DataDrivenDBInputSplit nullSplit =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) finalSplits.get(1);
    Assert.assertEquals("id IS NULL", nullSplit.getLowerClause());
    Assert.assertEquals("id IS NULL", nullSplit.getUpperClause());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfPresent() throws IOException {
    DataDrivenDBInputFormat.DataDrivenDBInputSplit existingSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("id >= 0", "id < 100");
    DataDrivenDBInputFormat.DataDrivenDBInputSplit nullSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("id IS NULL", "id IS NULL");
    List<InputSplit> initialSplits = ImmutableList.of(existingSplit, nullSplit);

    Mockito.doReturn(initialSplits).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);

    Assert.assertEquals("Should not add a duplicate NULL split", 2, finalSplits.size());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfSelectAllPresent() throws IOException {
    DataDrivenDBInputFormat.DataDrivenDBInputSplit existingSplit =
        new DataDrivenDBInputFormat.DataDrivenDBInputSplit("1=1", "1=1");
    List<InputSplit> initialSplits = ImmutableList.of(existingSplit);

    Mockito.doReturn(initialSplits).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);

    Assert.assertEquals("Should not add a NULL split", 1, finalSplits.size());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfBaseReturnsNull() throws IOException {
    Mockito.doReturn(null).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);
    Assert.assertEquals("Should not add a NULL split", 1, finalSplits.size());

    DataDrivenDBInputFormat.DataDrivenDBInputSplit split =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) finalSplits.get(0);
    Assert.assertEquals("1=1", split.getLowerClause());
  }

  @Test
  public void testGetSplitsDoesNotAddNullSplitIfBaseReturnsEmptyList() throws IOException {
    Mockito.doReturn(Collections.emptyList()).when(inputFormat).getBaseSplits(mockJobContext);

    List<InputSplit> finalSplits = inputFormat.getSplits(mockJobContext);
    Assert.assertEquals("Should not add a NULL split", 1, finalSplits.size());

    DataDrivenDBInputFormat.DataDrivenDBInputSplit split =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) finalSplits.get(0);
    Assert.assertEquals("1=1", split.getLowerClause());
  }

  @Test
  public void testDefaultConnectorInputFormatConfiguration() throws IOException {
    TestDBConnector connector = new TestDBConnector(new TestDBConnectorConfig());

    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    SampleRequest sampleRequest = SampleRequest.builder(10).setPath("db/table").build();
    InputFormatProvider provider = connector.getInputFormatProvider(context, sampleRequest);
    Map<String, String> conf = provider.getInputFormatConfiguration();

    Assert.assertEquals("false", conf.get(ConnectionConfigAccessor.AUTO_COMMIT_ENABLED));
    Assert.assertEquals("true", conf.get(ConnectionConfigAccessor.READ_ONLY));
  }

  @Test
  public void testGetConnectionReadOnlyDisablesAutoCommitBeforeReadOnlyAndWrapsFetchSize() throws Exception {
    Connection delegate = mockConnectionWithMetaData();
    Statement stmt = Mockito.mock(Statement.class);
    Mockito.when(delegate.createStatement()).thenReturn(stmt);
    Mockito.when(delegate.isClosed()).thenReturn(false);
    Mockito.when(delegate.getAutoCommit()).thenReturn(false);
    StubJdbcDriver.nextConnection = delegate;

    ConnectionConfigAccessor accessor = createConfigAccessor(true, false);
    accessor.setFetchSize(250);
    DataDrivenETLDBInputFormat format = createConfiguredInputFormat(accessor);

    Connection conn = format.getConnection();
    Assert.assertTrue(conn instanceof ConnectionWithFetchSize);
    Assert.assertSame(stmt, conn.createStatement());
    Mockito.verify(stmt).setFetchSize(250);

    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).setAutoCommit(false);
    inOrder.verify(delegate).setReadOnly(true);
    inOrder.verify(delegate).setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    // Proves ConnectionWithFetchSize wraps RollbackOnCloseConnection (commit is no-op; closeConnection rolls back).
    conn.commit();
    Mockito.verify(delegate, Mockito.never()).commit();
    Mockito.verify(delegate, Mockito.never()).rollback();

    Assert.assertNotNull(DriverManager.getDriver(STUB_URL));
    format.closeConnection();
    Mockito.verify(delegate).rollback();
    Mockito.verify(delegate).close();
    Mockito.verify(delegate, Mockito.never()).commit();
    Assert.assertThrows(SQLException.class, () -> DriverManager.getDriver(STUB_URL));
  }

  @Test
  public void testGetConnectionReadOnlyToleratesUnsupportedSetReadOnly() throws Exception {
    Connection delegate = mockConnectionWithMetaData();
    Mockito.doThrow(new SQLFeatureNotSupportedException("readOnly unsupported")).when(delegate).setReadOnly(true);
    StubJdbcDriver.nextConnection = delegate;

    DataDrivenETLDBInputFormat format = createConfiguredInputFormat(createConfigAccessor(true, false));

    Connection conn = format.getConnection();
    Assert.assertTrue(conn instanceof RollbackOnCloseConnection);
    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).setAutoCommit(false);
    inOrder.verify(delegate).setReadOnly(true);
  }

  @Test
  public void testGetConnectionReadOnlyToleratesUnsupportedSetAutoCommitAndFallsBackToNoOpCommit() throws Exception {
    Connection delegate = mockConnectionWithMetaData();
    Mockito.doThrow(new SQLFeatureNotSupportedException("autoCommit unsupported")).when(delegate).setAutoCommit(false);
    StubJdbcDriver.nextConnection = delegate;

    DataDrivenETLDBInputFormat format = createConfiguredInputFormat(createConfigAccessor(true, false));

    Connection conn = format.getConnection();
    Assert.assertTrue(conn instanceof NoOpCommitConnection);
    Assert.assertFalse(conn instanceof RollbackOnCloseConnection);
    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).setAutoCommit(false);
    inOrder.verify(delegate).setReadOnly(true);
  }

  @Test
  public void testGetConnectionReadOnlyWithAutoCommitEnabledSkipsSetAutoCommit() throws Exception {
    Connection delegate = mockConnectionWithMetaData();
    StubJdbcDriver.nextConnection = delegate;

    DataDrivenETLDBInputFormat format = createConfiguredInputFormat(createConfigAccessor(true, true));

    Connection conn = format.getConnection();
    Assert.assertTrue(conn instanceof NoOpCommitConnection);
    Assert.assertFalse(conn instanceof RollbackOnCloseConnection);
    Mockito.verify(delegate).setReadOnly(true);
    Mockito.verify(delegate, Mockito.never()).setAutoCommit(Mockito.anyBoolean());
  }

  @Test
  public void testGetConnectionDefaultWritableBatchSourcePath() throws Exception {
    Connection delegate = mockConnectionWithMetaData();
    StubJdbcDriver.nextConnection = delegate;

    DataDrivenETLDBInputFormat format = createConfiguredInputFormat(createConfigAccessor(false, false));

    Connection conn = format.getConnection();
    Assert.assertSame(delegate, conn);
    Mockito.verify(delegate).setAutoCommit(false);
    Mockito.verify(delegate, Mockito.never()).setReadOnly(Mockito.anyBoolean());
  }

  @Test
  public void testGetConnectionSetupFailureClosesConnectionAndDeregistersDriverShim() throws Exception {
    Connection failingDelegate = mockConnectionWithMetaData();
    Statement failingStmt = Mockito.mock(Statement.class);
    Mockito.when(failingDelegate.isClosed()).thenReturn(false);
    Mockito.when(failingDelegate.getAutoCommit()).thenReturn(false);
    Mockito.when(failingDelegate.createStatement()).thenReturn(failingStmt);
    Mockito.when(failingStmt.execute("SET BAD")).thenThrow(new SQLException("bad init query"));
    StubJdbcDriver.nextConnection = failingDelegate;

    ConnectionConfigAccessor accessor = createConfigAccessor(true, false);
    accessor.setInitQueries(Collections.singletonList("SET BAD"));
    DataDrivenETLDBInputFormat format = new DataDrivenETLDBInputFormat();

    // DBInputFormat.setConf invokes createConnection() -> getConnection().
    RuntimeException ex = Assert.assertThrows(RuntimeException.class,
                                              () -> format.setConf(accessor.getConfiguration()));
    Assert.assertTrue(ex.getCause() instanceof RuntimeException &&
                      ex.getCause().getCause() instanceof SQLException);

    InOrder cleanupOrder = Mockito.inOrder(failingDelegate);
    cleanupOrder.verify(failingDelegate).rollback();
    cleanupOrder.verify(failingDelegate).close();
    Assert.assertThrows(SQLException.class, () -> DriverManager.getDriver(STUB_URL));

    // A subsequent setConf()/getConnection() call does not return a half-configured connection and re-registers shim.
    Connection healthyDelegate = mockConnectionWithMetaData();
    Statement healthyStmt = Mockito.mock(Statement.class);
    Mockito.when(healthyDelegate.createStatement()).thenReturn(healthyStmt);
    StubJdbcDriver.nextConnection = healthyDelegate;

    format.setConf(accessor.getConfiguration());
    Connection retryConn = format.getConnection();
    Assert.assertTrue(retryConn instanceof RollbackOnCloseConnection);
    Mockito.verify(healthyDelegate).createStatement();
  }

  @Test
  public void testCreateDBRecordReaderCloseDeregistersDriverShimWhenCloseThrows() throws Exception {
    Connection delegate = mockConnectionWithMetaData();
    Mockito.when(delegate.isClosed()).thenReturn(false);
    Mockito.when(delegate.getAutoCommit()).thenReturn(false);
    Mockito.doThrow(new SQLException("close failed")).when(delegate).close();
    StubJdbcDriver.nextConnection = delegate;

    ConnectionConfigAccessor accessor = createConfigAccessor(true, false);
    Configuration conf = accessor.getConfiguration();
    DataDrivenETLDBInputFormat.setInput(conf, DBRecord.class, "SELECT * FROM tbl", null, false);
    DataDrivenETLDBInputFormat format = createConfiguredInputFormat(accessor);

    RecordReader reader = format.createDBRecordReader(
      new DataDrivenDBInputFormat.DataDrivenDBInputSplit("1=1", "1=1"), conf);
    Assert.assertNotNull(DriverManager.getDriver(STUB_URL));

    Assert.assertThrows(IOException.class, reader::close);
    Mockito.verify(delegate).rollback();
    Mockito.verify(delegate).close();
    Assert.assertThrows(SQLException.class, () -> DriverManager.getDriver(STUB_URL));
  }

  private static Connection mockConnectionWithMetaData() throws SQLException {
    Connection delegate = Mockito.mock(Connection.class);
    DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
    Mockito.when(delegate.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getDatabaseProductName()).thenReturn("PostgreSQL");
    return delegate;
  }

  private static ConnectionConfigAccessor createConfigAccessor(boolean readOnly, boolean autoCommit) {
    Configuration conf = new Configuration();
    conf.setClassLoader(DataDrivenETLDBInputFormatTest.class.getClassLoader());
    DBConfiguration.configureDB(conf, StubJdbcDriver.class.getName(), STUB_URL);
    new DBConfiguration(conf).setInputClass(DBRecord.class);
    ConnectionConfigAccessor accessor = new ConnectionConfigAccessor(conf);
    accessor.setReadOnly(readOnly);
    accessor.setAutoCommitEnabled(autoCommit);
    accessor.setConnectionArguments(Collections.emptyMap());
    return accessor;
  }

  private static DataDrivenETLDBInputFormat createConfiguredInputFormat(ConnectionConfigAccessor accessor) {
    DataDrivenETLDBInputFormat format = new DataDrivenETLDBInputFormat();
    format.setConf(accessor.getConfiguration());
    return format;
  }

  private static void deregisterStubDrivers() throws SQLException {
    for (Driver d : Collections.list(DriverManager.getDrivers())) {
      if (d.acceptsURL(STUB_URL)) {
        DriverManager.deregisterDriver(d);
      }
    }
  }

  public static class StubJdbcDriver implements Driver {
    static Connection nextConnection;

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) {
        return null;
      }
      if (nextConnection == null) {
        throw new SQLException("No stub connection configured");
      }
      return nextConnection;
    }

    @Override
    public boolean acceptsURL(String url) {
      return STUB_URL.equals(url);
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

  private static class TestDBConnectorConfig extends AbstractDBConnectorConfig {
    @Override
    public String getConnectionString() {
      return "jdbc:test://localhost:1234/db";
    }
  }

  private static class TestDBConnector extends AbstractDBSpecificConnector<DBRecord> {
    TestDBConnector(AbstractDBConnectorConfig config) {
      super(config);
      this.driverClass = Driver.class;
    }

    @Override
    public boolean supportSchema() {
      return false;
    }

    @Override
    protected Class<? extends DBWritable> getDBRecordType() {
      return DBRecord.class;
    }

    @Override
    public StructuredRecord transform(LongWritable key, DBRecord val) {
      return null;
    }

    @Override
    protected Connection getConnection(DBConnectorPath path) {
      return null;
    }

    @Override
    protected Schema loadTableSchema(Connection connection, String query,
                                     Integer timeoutSec, String sessionID) {
      return Schema.recordOf("outputSchema", Schema.Field.of("id", Schema.of(Schema.Type.INT)));
    }
  }
}

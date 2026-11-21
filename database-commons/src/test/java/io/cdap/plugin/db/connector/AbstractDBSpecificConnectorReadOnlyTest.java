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
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.common.db.DBPath;
import io.cdap.plugin.db.DBRecord;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Unit and embedded-JDBC tests verifying identifier escaping, read-only transactions, and rollback behavior on
 * {@link AbstractDBSpecificConnector}.
 */
public class AbstractDBSpecificConnectorReadOnlyTest {

  @Test
  public void loadTableSchema_defaultConnector_setsReadOnlyDisablesAutoCommitAndRollsBack() throws Exception {
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery("SELECT * FROM \"db\".\"tbl\"")).thenReturn(resultSet);
    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getColumnCount()).thenReturn(1);
    Mockito.when(metaData.getColumnName(1)).thenReturn("id");
    Mockito.when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
    Mockito.when(metaData.getColumnTypeName(1)).thenReturn("INTEGER");
    Mockito.when(metaData.isSigned(1)).thenReturn(true);
    Mockito.when(metaData.isNullable(1)).thenReturn(ResultSetMetaData.columnNoNulls);

    StubConnector connector = new StubConnector(connection, false);
    Schema schema = connector.loadTableSchema(connection, "SELECT * FROM \"db\".\"tbl\"", 5, "s1");
    Assert.assertNotNull(schema.getField("id"));

    InOrder inOrder = Mockito.inOrder(connection, statement, resultSet);
    inOrder.verify(connection).setReadOnly(true);
    inOrder.verify(connection).setAutoCommit(false);
    inOrder.verify(connection).createStatement();
    inOrder.verify(statement).setMaxRows(1);
    inOrder.verify(statement).setQueryTimeout(5);
    inOrder.verify(statement).executeQuery("SELECT * FROM \"db\".\"tbl\"");
    inOrder.verify(resultSet).close();
    inOrder.verify(statement).close();
    inOrder.verify(connection).rollback();
    Mockito.verify(connection, Mockito.never()).commit();
  }

  @Test
  public void loadTableSchema_queryFails_stillRollsBackAndNeverCommits() throws Exception {
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery(Mockito.anyString()))
      .thenThrow(new SQLException("Multiple ResultSets were returned by the query."));

    StubConnector connector = new StubConnector(connection, false);
    Assert.assertThrows(RuntimeException.class,
                        () -> connector.loadTableSchema(connection, "SELECT * FROM \"db\".\"tbl\"", null, "s1"));

    InOrder inOrder = Mockito.inOrder(connection, statement);
    inOrder.verify(connection).setReadOnly(true);
    inOrder.verify(connection).setAutoCommit(false);
    inOrder.verify(statement).executeQuery("SELECT * FROM \"db\".\"tbl\"");
    inOrder.verify(statement).close();
    inOrder.verify(connection).rollback();
    Mockito.verify(connection, Mockito.never()).commit();
  }

  @Test
  public void loadTableSchema_autoCommitConnectorAndUnsupportedReadOnly_toleratesAndSkipsRollback() throws Exception {
    Connection connection = mockConnectionWithSingleIntColumn();
    Mockito.doThrow(new SQLFeatureNotSupportedException("readOnly not supported")).when(connection).setReadOnly(true);

    StubConnector connector = new StubConnector(connection, true);
    Schema schema = connector.loadTableSchema(connection, "SELECT * FROM `db`.`tbl`", null, "s1");
    Assert.assertNotNull(schema.getField("id"));

    Mockito.verify(connection).setReadOnly(true);
    Mockito.verify(connection, Mockito.never()).setAutoCommit(Mockito.anyBoolean());
    Mockito.verify(connection, Mockito.never()).rollback();
    Mockito.verify(connection, Mockito.never()).commit();
  }

  @Test
  public void getInputFormatProvider_andGetTableSchema_closeOpenedConnections() throws Exception {
    Connection sampleConnection = mockConnectionWithSingleIntColumn();
    StubConnector connector = new StubConnector(sampleConnection, false);

    SampleRequest request = SampleRequest.builder(10).setPath("db/tbl").build();
    InputFormatProvider provider =
      connector.getInputFormatProvider(new MockConnectorContext(new MockConnectorConfigurer()), request);
    Assert.assertNotNull(provider);
    Mockito.verify(sampleConnection).rollback();
    Mockito.verify(sampleConnection).close();

    Connection specConnection = mockConnectionWithSingleIntColumn();
    StubConnector specConnector = new StubConnector(specConnection, false);
    Schema schema = specConnector.getTableSchema(null, "db", null, "tbl");
    Assert.assertNotNull(schema.getField("id"));
    Mockito.verify(specConnection).rollback();
    Mockito.verify(specConnection).close();
  }

  @Test
  public void sampleAndGenerateSpec_withQuoteAndStackedUpdateInPath_neverExecutesSecondStatement() throws Exception {
    String url = "jdbc:hsqldb:mem:vrp_" + UUID.randomUUID().toString().replace('-', '_');
    try (Connection adminConn = DriverManager.getConnection(url, "SA", "");
         Statement stmt = adminConn.createStatement()) {
      stmt.execute("CREATE TABLE \"PUBLIC\".\"target_table\" (\"id\" INT, \"name\" VARCHAR(32))");
      stmt.execute("INSERT INTO \"PUBLIC\".\"target_table\" VALUES (1, 'alice')");
      stmt.execute("CREATE TABLE \"PUBLIC\".\"write_marker\" (\"id\" INT PRIMARY KEY, \"val\" VARCHAR(64))");
      stmt.execute("INSERT INTO \"PUBLIC\".\"write_marker\" VALUES (1, 'ORIGINAL')");

      HsqldbSchemaConnector connector = new HsqldbSchemaConnector(url);
      MockConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());

      // Benign path succeeds through both generateSpec and full InputFormat/RecordReader sampling.
      ConnectorSpec benignSpec = connector.generateSpec(
        context, ConnectorSpecRequest.builder().setPath("/PUBLIC/target_table").setConnection("conn").build());
      Assert.assertNotNull(benignSpec.getSchema().getField("id"));
      InputFormatProvider provider = connector.getInputFormatProvider(
        context, SampleRequest.builder(10).setPath("/PUBLIC/target_table").build());
      List<StructuredRecord> records = readSampleRecords(connector, provider);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("alice", records.get(0).get("name"));
      Assert.assertEquals("ORIGINAL", readMarker(adminConn));

      // Injection payload with embedded '"' and stacked UPDATE stays inside the quoted identifier and cannot modify DB.
      String injectedPath = "/PUBLIC/target_table\" LIMIT 0; " +
        "UPDATE \"PUBLIC\".\"write_marker\" SET \"val\"='INJECTED_BY_SAMPLE_PATH' WHERE \"id\"=1; " +
        "SELECT * FROM \"PUBLIC\".\"target_table";
      Assert.assertThrows(RuntimeException.class, () -> connector.generateSpec(
        context, ConnectorSpecRequest.builder().setPath(injectedPath).setConnection("conn").build()));
      Assert.assertThrows(RuntimeException.class, () -> connector.getInputFormatProvider(
        context, SampleRequest.builder(10).setPath(injectedPath).build()));
      Assert.assertEquals("ORIGINAL", readMarker(adminConn));

      // Defense-in-depth: even if a write statement were passed directly to loadTableSchema, the read-only
      // transaction / rollback backstop prevents any modification from persisting.
      try (Connection connectorConn = DriverManager.getConnection(url, "SA", "")) {
        String updateSql = "UPDATE \"PUBLIC\".\"write_marker\" SET \"val\"='INJECTED_DIRECT' WHERE \"id\"=1";
        Assert.assertThrows(RuntimeException.class,
                            () -> connector.loadTableSchema(connectorConn, updateSql, null, "s1"));
      }
      Assert.assertEquals("ORIGINAL", readMarker(adminConn));
    } finally {
      try (Connection cleanup = DriverManager.getConnection(url, "SA", "");
           Statement stmt = cleanup.createStatement()) {
        stmt.execute("SHUTDOWN");
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static List<StructuredRecord> readSampleRecords(HsqldbSchemaConnector connector,
                                                          InputFormatProvider provider) throws Exception {
    Configuration hConf = new Configuration();
    hConf.setClassLoader(AbstractDBSpecificConnectorReadOnlyTest.class.getClassLoader());
    provider.getInputFormatConfiguration().forEach(hConf::set);
    Job job = Job.getInstance(hConf);
    job.setJobID(new JobID("sample", 0));
    InputFormat inputFormat = (InputFormat) AbstractDBSpecificConnectorReadOnlyTest.class.getClassLoader()
      .loadClass(provider.getInputFormatClassName()).newInstance();
    if (inputFormat instanceof Configurable) {
      ((Configurable) inputFormat).setConf(hConf);
    }
    List<InputSplit> splits = inputFormat.getSplits(job);
    InputSplit split = splits.get(0);
    TaskID taskId = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(hConf, new TaskAttemptID(taskId, 0));
    List<StructuredRecord> sample = new ArrayList<>();
    try (RecordReader<LongWritable, DBRecord> reader = inputFormat.createRecordReader(split, taskContext)) {
      reader.initialize(split, taskContext);
      while (reader.nextKeyValue()) {
        sample.add(connector.transform(reader.getCurrentKey(), reader.getCurrentValue()));
      }
    }
    return sample;
  }

  private static String readMarker(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT \"val\" FROM \"PUBLIC\".\"write_marker\" WHERE \"id\" = 1")) {
      Assert.assertTrue(rs.next());
      return rs.getString(1);
    }
  }

  private static Connection mockConnectionWithSingleIntColumn() throws SQLException {
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getColumnCount()).thenReturn(1);
    Mockito.when(metaData.getColumnName(1)).thenReturn("id");
    Mockito.when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
    Mockito.when(metaData.getColumnTypeName(1)).thenReturn("INTEGER");
    Mockito.when(metaData.isSigned(1)).thenReturn(true);
    Mockito.when(metaData.isNullable(1)).thenReturn(ResultSetMetaData.columnNoNulls);
    return connection;
  }

  private static class StubConnectorConfig extends AbstractDBConnectorConfig {
    private final String connectionString;

    StubConnectorConfig() {
      this("jdbc:stub://localhost:5432/db");
    }

    StubConnectorConfig(String connectionString) {
      this.connectionString = connectionString;
      this.user = "SA";
      this.password = "";
    }

    @Override
    public String getConnectionString() {
      return connectionString;
    }

    @Override
    public Integer getMaxRetryCount() {
      return 0;
    }
  }

  private static class StubConnector extends AbstractDBSpecificConnector<DBRecord> {
    private final Connection stubConnection;
    private final boolean autoCommitEnabled;

    StubConnector(Connection stubConnection, boolean autoCommitEnabled) {
      super(new StubConnectorConfig());
      this.driverClass = Driver.class;
      this.stubConnection = stubConnection;
      this.autoCommitEnabled = autoCommitEnabled;
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
    protected Connection getConnection() {
      return stubConnection;
    }

    @Override
    protected Connection getConnection(DBConnectorPath path) {
      return stubConnection;
    }

    @Override
    protected boolean isAutoCommitEnabled() {
      return autoCommitEnabled;
    }
  }

  private static class HsqldbSchemaConnector extends AbstractDBSpecificConnector<DBRecord> {
    HsqldbSchemaConnector(String url) {
      super(new StubConnectorConfig(url));
      this.driverClass = JDBCDriver.class;
    }

    @Override
    public boolean supportSchema() {
      return true;
    }

    @Override
    protected DBConnectorPath getDBConnectorPath(String path) throws IOException {
      return new DBPath(path, true);
    }

    @Override
    protected String getTableName(String database, String schema, String table) {
      return quoteIdentifier(schema, '"') + "." + quoteIdentifier(table, '"');
    }

    @Override
    protected Class<? extends DBWritable> getDBRecordType() {
      return DBRecord.class;
    }

    @Override
    public StructuredRecord transform(LongWritable key, DBRecord val) {
      return val.getRecord();
    }
  }
}

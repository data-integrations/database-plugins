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
import io.cdap.plugin.db.TransactionIsolationLevel;
import io.cdap.plugin.db.connector.AbstractDBConnectorConfig;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class DataDrivenETLDBInputFormatTest {

  @Mock
  private JobContext mockJobContext;
  @Mock
  private DBConfiguration mockDbConfiguration;

  private DataDrivenETLDBInputFormat inputFormat;

  @Before
  public void setUp() {
    inputFormat = Mockito.spy(new DataDrivenETLDBInputFormat());
    Mockito.doReturn(mockDbConfiguration).when(inputFormat).getDBConf();
    Mockito.doReturn("id").when(mockDbConfiguration).getInputOrderBy();
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

    Assert.assertFalse(connector.getBaseAutoCommitEnabled());
    Assert.assertNull(connector.getBaseTransactionIsolationLevel());

    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    SampleRequest sampleRequest = SampleRequest.builder(10).setPath("db/table").build();
    InputFormatProvider provider = connector.getInputFormatProvider(context, sampleRequest);
    Map<String, String> conf = provider.getInputFormatConfiguration();

    Assert.assertEquals("false", conf.get(ConnectionConfigAccessor.AUTO_COMMIT_ENABLED));
    Assert.assertNull(conf.get(TransactionIsolationLevel.CONF_KEY));
  }

  @Test
  public void testOverridingConnectorInputFormatConfiguration() throws IOException {
    TestDBConnector connector = new TestDBConnector(new TestDBConnectorConfig()) {
      @Override
      protected boolean isAutoCommitEnabled() {
        return true;
      }

      @Override
      protected String getTransactionIsolationLevel() {
        return TransactionIsolationLevel.Level.TRANSACTION_READ_UNCOMMITTED.name();
      }
    };

    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    SampleRequest sampleRequest = SampleRequest.builder(10).setPath("db/table").build();
    InputFormatProvider provider = connector.getInputFormatProvider(context, sampleRequest);
    Map<String, String> conf = provider.getInputFormatConfiguration();

    Assert.assertEquals("true", conf.get(ConnectionConfigAccessor.AUTO_COMMIT_ENABLED));
    Assert.assertEquals(TransactionIsolationLevel.Level.TRANSACTION_READ_UNCOMMITTED.name(),
                        conf.get(TransactionIsolationLevel.CONF_KEY));
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

    boolean getBaseAutoCommitEnabled() {
      return isAutoCommitEnabled();
    }

    String getBaseTransactionIsolationLevel() {
      return getTransactionIsolationLevel();
    }
  }
}

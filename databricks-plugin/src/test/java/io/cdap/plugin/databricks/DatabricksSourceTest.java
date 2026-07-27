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

import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.TransactionIsolationLevel;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

/**
 * Unit tests for {@link DatabricksSource}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DatabricksSourceTest {

  private DatabricksConnectorConfig createConnectorConfig() {
    return new DatabricksConnectorConfig("token", "password", "jdbcPluginName", "connectionArguments",
                                         "dbc-xxx.cloud.databricks.com", "sql/1.0/warehouses/xxx", "main", 443);
  }

  @Test
  public void testGetDBSpecificArguments() {
    DatabricksSource.DatabricksSourceConfig config =
      new DatabricksSource.DatabricksSourceConfig(false, createConnectorConfig());
    Map<String, String> dbSpecificArguments = config.getDBSpecificArguments();
    Assert.assertEquals(0, dbSpecificArguments.size());
  }

  @Test
  public void testGetFetchSize() {
    DatabricksSource.DatabricksSourceConfig config =
      new DatabricksSource.DatabricksSourceConfig(false, createConnectorConfig());
    Integer fetchSize = config.getFetchSize();
    Assert.assertEquals(1000, fetchSize.intValue());
  }

  @Test
  public void testGetTransactionIsolationLevel() {
    DatabricksSource.DatabricksSourceConfig config =
      new DatabricksSource.DatabricksSourceConfig(false, createConnectorConfig());
    Assert.assertEquals(TransactionIsolationLevel.Level.TRANSACTION_REPEATABLE_READ.name(),
                        config.getTransactionIsolationLevel());
  }

  @Test
  public void testGetSchemaReader() {
    DatabricksSource source =
      new DatabricksSource(new DatabricksSource.DatabricksSourceConfig(false, createConnectorConfig()));
    SchemaReader schemaReader = source.getSchemaReader();
    Assert.assertTrue(schemaReader instanceof DatabricksSchemaReader);
  }

  @Test
  public void testGetDBRecordType() {
    DatabricksSource source =
      new DatabricksSource(new DatabricksSource.DatabricksSourceConfig(false, createConnectorConfig()));
    Class<? extends DBWritable> dbRecordType = source.getDBRecordType();
    Assert.assertEquals(DatabricksDBRecord.class, dbRecordType);
  }

  @Test
  public void testCreateConnectionString() {
    DatabricksSource.DatabricksSourceConfig config =
      new DatabricksSource.DatabricksSourceConfig(false, createConnectorConfig());
    DatabricksSource source = new DatabricksSource(config);
    Assert.assertEquals(
      "jdbc:databricks://dbc-xxx.cloud.databricks.com:443;ConnCatalog=main;HttpPath=sql/1.0/warehouses/xxx;",
      source.createConnectionString());
  }

  @Test
  public void testGetLineageRecorder() {
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    DatabricksSource.DatabricksSourceConfig config =
      new DatabricksSource.DatabricksSourceConfig(false, createConnectorConfig());
    DatabricksSource source = new DatabricksSource(config);

    LineageRecorder lineageRecorder = source.getLineageRecorder(context);
    Assert.assertNotNull(lineageRecorder);
  }
}

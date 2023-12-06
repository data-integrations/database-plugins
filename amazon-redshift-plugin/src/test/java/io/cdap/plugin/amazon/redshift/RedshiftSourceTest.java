/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.amazon.redshift;

import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.db.SchemaReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class RedshiftSourceTest {

  @Test
  public void testGetDBSpecificArguments() {
    RedshiftConnectorConfig connectorConfig = new RedshiftConnectorConfig("username", "password",
                                                                          "jdbcPluginName", "connectionArguments",
                                                                          "host", "database", 1101);
    RedshiftSource.RedshiftSourceConfig config = new RedshiftSource.RedshiftSourceConfig(false, connectorConfig);
    Map<String, String> dbSpecificArguments = config.getDBSpecificArguments();
    Assert.assertEquals(0, dbSpecificArguments.size());
  }

  @Test
  public void testGetFetchSize() {
    RedshiftConnectorConfig connectorConfig = new RedshiftConnectorConfig("username", "password",
                                                                          "jdbcPluginName", "connectionArguments",
                                                                          "host", "database", 1101);
    RedshiftSource.RedshiftSourceConfig config = new RedshiftSource.RedshiftSourceConfig(false, connectorConfig);
    Integer fetchSize = config.getFetchSize();
    Assert.assertEquals(1000, fetchSize.intValue());
  }

  @Test
  public void testGetSchemaReader() {
    RedshiftConnectorConfig connectorConfig = new RedshiftConnectorConfig("username", "password",
                                                                          "jdbcPluginName", "connectionArguments",
                                                                          "host", "database", 1101);
    RedshiftSource source = new RedshiftSource(new RedshiftSource.RedshiftSourceConfig(false, connectorConfig));
    SchemaReader schemaReader = source.getSchemaReader();
    Assert.assertTrue(schemaReader instanceof RedshiftSchemaReader);
  }

  @Test
  public void testGetDBRecordType() {
    RedshiftConnectorConfig connectorConfig = new RedshiftConnectorConfig("username", "password",
                                                                          "jdbcPluginName", "connectionArguments",
                                                                          "host", "database", 1101);
    RedshiftSource source = new RedshiftSource(new RedshiftSource.RedshiftSourceConfig(false, connectorConfig));
    Class<? extends DBWritable> dbRecordType = source.getDBRecordType();
    Assert.assertEquals(RedshiftDBRecord.class, dbRecordType);
  }

  @Test
  public void testCreateConnectionString() {
    RedshiftConnectorConfig connectorConfig = new RedshiftConnectorConfig("username", "password",
                                                                          "jdbcPluginName", "connectionArguments",
                                                                          "localhost", "test", 5439);
    RedshiftSource.RedshiftSourceConfig config = new RedshiftSource.RedshiftSourceConfig(false, connectorConfig);

    RedshiftSource source = new RedshiftSource(config);
    String connectionString = source.createConnectionString();
    Assert.assertEquals("jdbc:redshift://localhost:5439/test", connectionString);
  }

  @Test
  public void testGetLineageRecorder() {
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    RedshiftConnectorConfig connectorConfig = new RedshiftConnectorConfig("username", "password",
                                                                          "jdbcPluginName", "connectionArguments",
                                                                          "host", "database", 1101);
    RedshiftSource.RedshiftSourceConfig config = new RedshiftSource.RedshiftSourceConfig(false, connectorConfig);
    RedshiftSource source = new RedshiftSource(config);

    LineageRecorder lineageRecorder = source.getLineageRecorder(context);
    Assert.assertNotNull(lineageRecorder);
  }
}

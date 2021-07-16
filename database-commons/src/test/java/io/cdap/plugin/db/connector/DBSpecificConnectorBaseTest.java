/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base integration test for DB Specific Connector, it will only be run when below properties are provided:
 * -Dhost -- the host name or ip address of the database server
 * -Dport -- the port number of the database server
 * -Dusername -- the username used to connect to database
 * -Dpassword -- the password used to connect to database
 * -Ddatabase.name -- the database name
 * -Dschema.name -- the schema name, optional for those DBs that don't support schema
 * -Dtable.name -- the table name
 * -Dconnection.arguments -- the additional connection arguments, optional
 */
public abstract class DBSpecificConnectorBaseTest {
  protected static final String JDBC_PLUGIN_NAME = "jdbc_plugin";
  protected static String host;
  protected static int port;
  protected static String username;
  protected static String password;
  protected static String connectionArguments;
  protected static String table;
  protected static String schema;
  protected static String database;
  private static final String NAME_USE_CONNECTION = "useConnection";
  private static final String NAME_CONNECTION = "connection";
  private static final String IMPORT_QUERY = "importQuery";
  private static final String NUM_SPLITS = "numSplits";

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.

    String messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";

    host = System.getProperty("host");
    Assume.assumeFalse(String.format(messageTemplate, "host"), host == null);

    String portStr = System.getProperty("port");
    port = Integer.parseInt(portStr);

    username = System.getProperty("username");
    Assume.assumeFalse(String.format(messageTemplate, "username"), username == null);

    password = System.getProperty("password");
    Assume.assumeFalse(String.format(messageTemplate, "password"), password == null);

    database = System.getProperty("database.name");
    Assume.assumeFalse(String.format(messageTemplate, "database name"), database == null);

    schema = System.getProperty("schema.name");

    table = System.getProperty("table.name");
    Assume.assumeFalse(String.format(messageTemplate, "table name"), table == null);

    connectionArguments = System.getProperty("connection.arguments");
  }

  protected void testSample(AbstractDBSpecificConnector connector)
    throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {


    List<StructuredRecord> sample = sample(connector, new MockConnectorContext(new MockConnectorConfigurer()),
                                           SampleRequest.builder(1).setPath(schema == null ? database + "/" + table :
                                                                              database + "/" + schema + "/" + table)
                                             .build());
    Assert.assertTrue(sample.size() > 0);
    StructuredRecord record = sample.get(0);
    Schema tableSchema = record.getSchema();
    Assert.assertNotNull(tableSchema);
    for (Schema.Field field : tableSchema.getFields()) {
      Assert.assertNotNull(field.getSchema());
      Assert.assertTrue(record.get(field.getName()) != null || field.getSchema().isNullable());
    }

    //invalid path
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> sample(connector, new MockConnectorContext(new MockConnectorConfigurer()),
                                     SampleRequest.builder(1).setPath(schema == null ? "a/b/c" : "a/b/c/d")
                                       .build()));


    if (schema != null) {
      //sample tableSchema
      Assert.assertThrows(IllegalArgumentException.class,
                          () -> sample(connector, new MockConnectorContext(new MockConnectorConfigurer()),
                                       SampleRequest.builder(1).setPath(schema).build()));
    }
  }

  protected void testBrowse(AbstractDBSpecificConnector connector) throws IOException {
    // browse DB server to list database
    BrowseDetail detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                           BrowseRequest.builder("/").build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      System.out.println(entity.getType() + " : " + entity.getName());
      Assert.assertEquals("DATABASE", entity.getType());
      Assert.assertTrue(entity.canBrowse());
      Assert.assertFalse(entity.canSample());
    }

    if (schema != null) {
      // browse database to list schema
      detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                BrowseRequest.builder("/" + database).build());
      Assert.assertTrue(detail.getTotalCount() > 0);
      Assert.assertTrue(detail.getEntities().size() > 0);
      for (BrowseEntity entity : detail.getEntities()) {
        System.out.println(entity.getType() + " : " + entity.getName());
        Assert.assertEquals("SCHEMA", entity.getType());
        Assert.assertTrue(entity.canBrowse());
        Assert.assertFalse(entity.canSample());
      }
      // browse schema to list tables
      detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                BrowseRequest.builder("/" + database + "/" + schema).build());
      Assert.assertTrue(detail.getTotalCount() > 0);
      Assert.assertTrue(detail.getEntities().size() > 0);
      for (BrowseEntity entity : detail.getEntities()) {
        System.out.println(entity.getType() + " : " + entity.getName());
        Assert.assertFalse(entity.canBrowse());
        Assert.assertTrue(entity.canSample());
      }

    } else {

      // browse database to list tables
      detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                BrowseRequest.builder("/" + database).build());
      Assert.assertTrue(detail.getTotalCount() > 0);
      Assert.assertTrue(detail.getEntities().size() > 0);
      for (BrowseEntity entity : detail.getEntities()) {
        System.out.println(entity.getType() + " : " + entity.getName());
        Assert.assertFalse(entity.canBrowse());
        Assert.assertTrue(entity.canSample());
      }
    }

    // browse table
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(
                                schema == null ? database + "/" + table : database + "/" + schema + "/" + table)
                                .build());
    Assert.assertEquals(1, detail.getTotalCount());
    Assert.assertEquals(1, detail.getEntities().size());
    for (BrowseEntity entity : detail.getEntities()) {
      System.out.println(entity.getType() + " : " + entity.getName());
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }

    // invalid path
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder(schema == null ? "a/b/c" : "a/b/c/d").build()));

    // not existing schema or table
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder("/notexisting").build()));

    if (schema != null) {
      // not existing table
      Assert.assertThrows(IllegalArgumentException.class,
                          () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                                 BrowseRequest.builder(schema + "/notexisting").build()));
    }
  }

  protected void testTest(AbstractDBSpecificConnector connector) {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(context);
    ValidationException validationException = context.getFailureCollector().getOrThrowException();
    Assert.assertTrue(validationException.getFailures().isEmpty());
  }

  protected void testGenerateSpec(AbstractDBSpecificConnector connector, String sourceName) throws IOException {
    ConnectorSpec connectorSpec = connector.generateSpec(new MockConnectorContext((new MockConnectorConfigurer())),
                                                         ConnectorSpecRequest.builder()
                                                           .setPath(
                                                             schema == null ? database + "/" + table :
                                                               database + "/" + schema + "/" + table)
                                                           .setConnection("${conn(connection-id)}").build());
    Schema tableSchema = connectorSpec.getSchema();
    for (Schema.Field field : tableSchema.getFields()) {
      Assert.assertNotNull(field.getSchema());
    }
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(1, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(sourceName, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());

    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(NAME_CONNECTION));
    Assert.assertEquals(schema == null ? String.format("SELECT * FROM %s.%s", database, table) :
                          String.format("SELECT * FROM %s.%s.%s", database, schema, table),
                        properties.get(IMPORT_QUERY));
    properties.put("1", properties.get(NUM_SPLITS));
  }

  protected List<StructuredRecord> sample(BatchConnector batchConnector, ConnectorContext context,
                                          SampleRequest request)
    throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    InputFormatProvider inputFormatProvider = batchConnector.getInputFormatProvider(context, request);

    Configuration hConf = new Configuration();
    hConf.setClassLoader(DBSpecificConnectorBaseTest.class.getClassLoader());
    inputFormatProvider.getInputFormatConfiguration().forEach(hConf::set);
    Job job = Job.getInstance(hConf);
    job.setJobID(new JobID("sample", 0));
    List<InputSplit> splits;
    InputFormat inputFormat = (InputFormat) DBSpecificConnectorBaseTest.class.getClassLoader()
      .loadClass(inputFormatProvider.getInputFormatClassName()).newInstance();
    if (inputFormat instanceof Configurable) {
      ((Configurable) inputFormat).setConf(hConf);
    }
    try {
      splits = inputFormat.getSplits(job);
    } catch (InterruptedException e) {
      throw new IOException(String.format("Unable to get the splits from the input format %s",
                                          inputFormatProvider.getInputFormatClassName()));
    }
    List<StructuredRecord> sample = new ArrayList<>();
    InputSplit split = splits.get(0);
    TaskID taskId = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(hConf, new TaskAttemptID(taskId, 0));

    // create record reader to read the results
    try (RecordReader<?, ?> reader = inputFormat.createRecordReader(split, taskContext)) {
      reader.initialize(split, taskContext);
      while (reader.nextKeyValue()) {
        sample.add(batchConnector.transform(reader.getCurrentKey(), reader.getCurrentValue()));
      }
    } catch (InterruptedException e) {
      throw new IOException(String.format("Unable to read the values from the input format %s",
                                          inputFormatProvider.getInputFormatClassName()));
    }
    return sample;
  }

  protected void test(AbstractDBSpecificConnector connector, String driverClassName, String sourcePluginName)
    throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    ConnectorConfigurer configurer = Mockito.mock(ConnectorConfigurer.class);
    Mockito.when(configurer.usePluginClass(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                                           Mockito.any(PluginProperties.class)))
      .thenReturn((Class) DBSpecificConnectorBaseTest.class.getClassLoader().loadClass(driverClassName));
    connector.configure(configurer);
    testTest(connector);
    testBrowse(connector);
    testSample(connector);
    testGenerateSpec(connector, sourcePluginName);
  }
}

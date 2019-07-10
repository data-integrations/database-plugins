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

package io.cdap.plugin.db.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base test class for all database plugins.
 */
public class DatabasePluginTestBase extends HydratorTestBase {

  public static Schema getSchemaWithInvalidTypeMapping(String columnName, Schema.Type type) {
    return Schema.recordOf(
      "wrongDBRecord",
      Schema.Field.of(columnName, Schema.of(type))
    );
  }

  protected static void assertDeploymentFailure(ApplicationId appId, ETLBatchConfig etlConfig,
                                                ArtifactSummary datapipelineArtifact, String  failureMessage)
    throws Exception {
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(datapipelineArtifact, etlConfig);
    try {
      deployApplication(appId, appRequest);
      Assert.fail(failureMessage);
    } catch (IllegalStateException e) {
      // expected
    }
  }

  protected static void assertRuntimeFailure(ApplicationId appId, ETLBatchConfig etlConfig,
                                             ArtifactSummary datapipelineArtifact, String failureMessage, int runCount)
    throws Exception {
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(datapipelineArtifact, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.FAILED, runCount, 3, TimeUnit.MINUTES);
  }

  protected ApplicationManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin,
                                         ArtifactSummary datapipelineArtifact, String appName)
    throws Exception {
    ETLBatchConfig etlConfig = getETLBatchConfig(sourcePlugin, sinkPlugin);
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(datapipelineArtifact, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  protected ETLBatchConfig getETLBatchConfig(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin) {
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    return ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
  }

  protected void runETLOnce(ApplicationManager appManager) throws TimeoutException,
    InterruptedException, ExecutionException {
    runETLOnce(appManager, ImmutableMap.<String, String>of());
  }

  protected void runETLOnce(ApplicationManager appManager,
                            Map<String, String> arguments) throws TimeoutException, InterruptedException,
    ExecutionException {
    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(arguments);
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }

  protected void testDBInvalidFieldType(String columnName, Schema.Type type, ETLPlugin sinkConfig,
                                        ArtifactSummary datapipelineArtifact) throws Exception {
    String inputDatasetName = "input-dbsinktest-invalid-field-type";
    Schema schema = getSchemaWithInvalidTypeMapping(columnName, type);
    testDBSinkValidation(inputDatasetName, "testDBSinkWithInvalidFieldType", schema, datapipelineArtifact,
                         sinkConfig);
  }

  protected void testDBInvalidFieldLogicalType(String columnName, Schema.Type type, ETLPlugin sinkConfig,
                                               ArtifactSummary datapipelineArtifact) throws Exception {
    String inputDatasetName = "input-dbsinktest-invalid-field-logical-type";
    Schema schema = getSchemaWithInvalidTypeMapping(columnName, type);
    testDBSinkValidation(inputDatasetName, "testDBSinkWithInvalidFieldLogicalType", schema,
                         datapipelineArtifact, sinkConfig);
  }

  protected void testDBSinkValidation(String inputDatasetName, String appName, Schema schema,
                                      ArtifactSummary datapipelineArtifact, ETLPlugin sinkConfig) throws Exception {
    ETLPlugin sourceConfig = MockSource.getPlugin(inputDatasetName, schema);
    ETLBatchConfig etlConfig = getETLBatchConfig(sourceConfig, sinkConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    assertDeploymentFailure(appId, etlConfig, datapipelineArtifact, "No fail message on schema validation");
  }

  protected void writeDataForInvalidDataWriteTest(String inputDatasetName, String stringColumnName) throws Exception {
    Schema validSchema = Schema.recordOf(
      "wrongDBRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of(stringColumnName, Schema.of(Schema.Type.STRING))
    );

    Schema invalidSchema = Schema.recordOf(
      "wrongDBRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of(stringColumnName, Schema.of(Schema.Type.INT))
    );

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);

    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(validSchema)
                       .set("ID", 1)
                       .set(stringColumnName, "user1")
                       .build());
    inputRecords.add(StructuredRecord.builder(invalidSchema)
                       .set("ID", 2)
                       .set(stringColumnName, 1)
                       .build());
    inputRecords.add(StructuredRecord.builder(validSchema)
                       .set("ID", 3)
                       .set(stringColumnName, "user3")
                       .build());
    MockSource.writeInput(inputManager, inputRecords);
  }

  protected void startPipelineAndWriteInvalidData(String stringColumnName, ETLPlugin sinkConfig,
                                                  ArtifactSummary datapipelineArtifact) throws Exception {
    String inputDatasetName = "input-dbsinktest-db-schema-invalid-schema-mapping";
    ETLPlugin sourceConfig = MockSource.getPlugin(inputDatasetName);
    ApplicationManager applicationManager =  deployETL(sourceConfig, sinkConfig, datapipelineArtifact,
                                                       "testDBSinkWithDBSchemaAndInvalidSchemaMapping");

    writeDataForInvalidDataWriteTest(inputDatasetName, stringColumnName);
    WorkflowManager workflowManager = applicationManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.FAILED, 5, TimeUnit.MINUTES);
  }

  protected void testInvalidDataWrite(ResultSet resultSet, String columnName) throws SQLException {
    List<String> users = new ArrayList<>();
    while (resultSet.next()) {
      users.add(resultSet.getString(columnName).trim());
    }
    Assert.assertFalse(users.contains("1"));
  }
}

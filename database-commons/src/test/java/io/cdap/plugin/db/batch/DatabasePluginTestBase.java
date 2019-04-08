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
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base test class for all database plugins.
 */
public class DatabasePluginTestBase extends HydratorTestBase {

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
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(datapipelineArtifact, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
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
}

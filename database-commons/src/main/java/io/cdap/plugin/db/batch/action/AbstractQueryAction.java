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

package io.cdap.plugin.db.batch.action;

import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.util.DBUtils;

import java.sql.Driver;

/**
 * Base class representing database post-action,
 * runs a query after a pipeline run.
 */
@SuppressWarnings("ConstantConditions")
public abstract class AbstractQueryAction extends PostAction {
  private static final String JDBC_PLUGIN_ID = "driver";
  private final QueryActionConfig config;
  private final Boolean enableAutoCommit;

  public AbstractQueryAction(QueryActionConfig config, Boolean enableAutoCommit) {
    this.config = config;
    this.enableAutoCommit = enableAutoCommit;
  }

  @Override
  public void run(BatchActionContext batchContext) throws Exception {
    config.validate();

    if (!config.shouldRun(batchContext)) {
      return;
    }

    Class<? extends Driver> driverClass = batchContext.loadPluginClass(JDBC_PLUGIN_ID);
    DBRun executeQuery = new DBRun(config, driverClass, enableAutoCommit);
    executeQuery.run();
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    config.validate();
    DBUtils.validateJDBCPluginPipeline(pipelineConfigurer, config, JDBC_PLUGIN_ID);
  }
}

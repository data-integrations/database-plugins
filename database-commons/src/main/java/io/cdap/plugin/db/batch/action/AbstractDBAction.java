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
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.util.DBUtils;

import java.sql.Driver;

/**
 * Action that runs a db command.
 */
public abstract class AbstractDBAction extends Action {
  private static final String JDBC_PLUGIN_ID = "driver";
  private final QueryConfig config;
  private final Boolean enableAutoCommit;

  public AbstractDBAction(QueryConfig config, Boolean enableAutoCommit) {
    this.config = config;
    this.enableAutoCommit = enableAutoCommit;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
    DBRun executeQuery = new DBRun(config, driverClass, enableAutoCommit);
    executeQuery.run();
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    DBUtils.validateJDBCPluginPipeline(pipelineConfigurer, config, JDBC_PLUGIN_ID);
  }
}

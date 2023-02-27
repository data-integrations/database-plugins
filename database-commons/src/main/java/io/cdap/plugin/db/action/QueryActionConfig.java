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

package io.cdap.plugin.db.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.plugin.common.batch.action.Condition;
import io.cdap.plugin.common.batch.action.ConditionConfig;

import javax.annotation.Nullable;

/**
 * Base config for database post-actions {@link AbstractQueryAction}
 */
public abstract class QueryActionConfig extends QueryConfig {
  public static final String RUN_CONDITION = "runCondition";

  @Nullable
  @Description("When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'success'. " +
    "If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or " +
    "failed. If set to 'success', the action will only be executed if the pipeline run succeeded. " +
    "If set to 'failure', the action will only be executed if the pipeline run failed.")
  @Macro
  public String runCondition;

  public QueryActionConfig() {
    super();
    runCondition = Condition.SUCCESS.name();
  }

  public void validate(FailureCollector collector) {
    // have to delegate instead of inherit, since we can't extend both ConditionConfig and ConnectionConfig.
    if (!containsMacro(RUN_CONDITION)) {
      try {
        new ConditionConfig(runCondition).validate();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(RUN_CONDITION);
      }
    }
  }

  public boolean shouldRun(BatchActionContext actionContext) {
    return new ConditionConfig(runCondition).shouldRun(actionContext);
  }
}

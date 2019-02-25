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

package co.cask.oracle;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.action.Action;
import co.cask.db.batch.action.AbstractDBAction;
import co.cask.db.batch.config.DBSpecificQueryConfig;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Action that runs Oracle command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(OracleConstants.PLUGIN_NAME)
@Description("Action that runs Oracle command")
public class OracleAction extends AbstractDBAction {

  private final OracleActionConfig oracleActionConfig;

  public OracleAction(OracleActionConfig oracleActionConfig) {
    super(oracleActionConfig, false);
    this.oracleActionConfig = oracleActionConfig;
  }

  /**
   * Oracle Action Config.
   */
  public static class OracleActionConfig extends DBSpecificQueryConfig {
    @Name(OracleConstants.DEFAULT_BATCH_VALUE)
    @Description("The default batch value that triggers an execution request.")
    public Integer defaultBatchValue;

    @Override
    public String getConnectionString() {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(OracleConstants.DEFAULT_BATCH_VALUE, String.valueOf(defaultBatchValue));
    }
  }
}

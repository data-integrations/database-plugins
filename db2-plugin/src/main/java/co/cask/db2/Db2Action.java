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

package co.cask.db2;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.action.Action;
import co.cask.db.batch.action.AbstractDBAction;
import co.cask.db.batch.config.DBSpecificQueryConfig;

/**
 * Action that runs DB2 command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(Db2Constants.PLUGIN_NAME)
@Description("Action that runs a DB2 command.")
public class Db2Action extends AbstractDBAction {

  private final Db2ActionConfig db2ActionConfig;

  public Db2Action(Db2ActionConfig db2ActionConfig) {
    super(db2ActionConfig, false);
    this.db2ActionConfig = db2ActionConfig;
  }

  /**
   * DB2 Action Config.
   */
  public static class Db2ActionConfig extends DBSpecificQueryConfig {
    @Override
    public String getConnectionString() {
      return String.format(Db2Constants.DB2_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }
}

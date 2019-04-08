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

package io.cdap.plugin.db2;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.db.batch.action.AbstractQueryAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryActionConfig;


/**
 * Represents DB2 post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(Db2Constants.PLUGIN_NAME)
@Description("Runs a DB2 query after a pipeline run.")
public class Db2PostAction extends AbstractQueryAction {

  private final Db2QueryActionConfig db2QueryActionConfig;

  public Db2PostAction(Db2QueryActionConfig db2QueryActionConfig) {
    super(db2QueryActionConfig, false);
    this.db2QueryActionConfig = db2QueryActionConfig;
  }

  /**
   * DB2 post action configuration.
   */
  public static class Db2QueryActionConfig extends DBSpecificQueryActionConfig {
    @Override
    public String getConnectionString() {
      return String.format(Db2Constants.DB2_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }
}

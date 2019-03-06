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

package co.cask.netezza;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.db.batch.action.AbstractQueryAction;
import co.cask.db.batch.config.DBSpecificQueryActionConfig;

/**
 * Represents Netezza post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(NetezzaConstants.PLUGIN_NAME)
@Description("Runs a Netezza query after a pipeline run.")
public class NetezzaPostAction extends AbstractQueryAction {

  private final NetezzaQueryActionConfig netezzaQueryActionConfig;

  public NetezzaPostAction(NetezzaQueryActionConfig netezzaQueryActionConfig) {
    super(netezzaQueryActionConfig, false);
    this.netezzaQueryActionConfig = netezzaQueryActionConfig;
  }

  /**
   * Netezza post action netezzaQueryActionConfig.
   */
  public static class NetezzaQueryActionConfig extends DBSpecificQueryActionConfig {

    @Override
    public String getConnectionString() {
      return String.format(NetezzaConstants.NETEZZA_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }
}

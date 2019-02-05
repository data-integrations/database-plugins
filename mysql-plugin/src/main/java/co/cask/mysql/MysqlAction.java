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

package co.cask.mysql;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.action.Action;
import co.cask.db.batch.action.AbstractDBAction;
import co.cask.db.batch.config.DBSpecificQueryConfig;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Action that runs MySQL command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(MysqlConstants.PLUGIN_NAME)
@Description("Action that runs a MySQL command")
public class MysqlAction extends AbstractDBAction {

  private final MysqlActionConfig mysqlActionConfig;

  public MysqlAction(MysqlActionConfig mysqlActionConfig) {
    super(mysqlActionConfig, false);
    this.mysqlActionConfig = mysqlActionConfig;
  }

  /**
   * Mysql Action Config.
   */
  public static class MysqlActionConfig extends DBSpecificQueryConfig {

    @Name(MysqlConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    public Boolean autoReconnect;

    @Override
    public String getConnectionString() {
      return String.format(MysqlConstants.MYSQL_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      builder.put(MysqlConstants.AUTO_RECONNECT, String.valueOf(autoReconnect));
      builder.put(MysqlConstants.ALLOW_MULTIPLE_QUERIES, String.valueOf(true));

      return builder.build();
    }
  }
}

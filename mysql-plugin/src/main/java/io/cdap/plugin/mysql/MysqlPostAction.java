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

package io.cdap.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.db.batch.action.AbstractQueryAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryActionConfig;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents MySQL post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(MysqlConstants.PLUGIN_NAME)
@Description("Runs a MySQL query after a pipeline run.")
public class MysqlPostAction extends AbstractQueryAction {

  private final MysqlQueryActionConfig mysqlQueryActionConfig;

  public MysqlPostAction(MysqlQueryActionConfig mysqlQueryActionConfig) {
    super(mysqlQueryActionConfig, false);
    this.mysqlQueryActionConfig = mysqlQueryActionConfig;
  }

  /**
   * MySQL post action mysqlQueryActionConfig.
   */
  public static class MysqlQueryActionConfig extends DBSpecificQueryActionConfig {

    @Name(MysqlConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Override
    public String getConnectionString() {
      return String.format(MysqlConstants.MYSQL_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      if (autoReconnect != null) {
        builder.put(MysqlConstants.AUTO_RECONNECT, String.valueOf(autoReconnect));
      }
      builder.put(MysqlConstants.ALLOW_MULTIPLE_QUERIES, String.valueOf(true));

      return builder.build();
    }
  }
}

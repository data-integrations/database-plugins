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

package io.cdap.plugin.auroradb.mysql;

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
 * Represents Aurora DB MySQL post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(AuroraMysqlConstants.PLUGIN_NAME)
@Description("Runs an Aurora DB query after a pipeline run.")
public class AuroraMysqlPostAction extends AbstractQueryAction {

  private final AuroraMysqlQueryActionConfig auroraMysqlQueryActionConfig;

  public AuroraMysqlPostAction(AuroraMysqlQueryActionConfig auroraMysqlQueryActionConfig) {
    super(auroraMysqlQueryActionConfig, false);
    this.auroraMysqlQueryActionConfig = auroraMysqlQueryActionConfig;
  }

  /**
   * Aurora DB MySQL post action configuration.
   */
  public static class AuroraMysqlQueryActionConfig extends DBSpecificQueryActionConfig {

    @Name(AuroraMysqlConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Override
    public String getConnectionString() {
      return String.format(AuroraMysqlConstants.AURORA_MYSQL_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      if (autoReconnect != null) {
        return ImmutableMap.of(AuroraMysqlConstants.AUTO_RECONNECT, String.valueOf(autoReconnect));
      } else {
        return ImmutableMap.of();
      }
    }
  }
}

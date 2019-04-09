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

package io.cdap.plugin.auroradb.postgres;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.batch.action.AbstractDBAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryConfig;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Action that runs Aurora DB PostgreSQL command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(AuroraPostgresConstants.PLUGIN_NAME)
@Description("Action that runs an Aurora DB command.")
public class AuroraPostgresAction extends AbstractDBAction {

  private final AuroraPostgresActionConfig auroraPostgresActionConfig;

  public AuroraPostgresAction(AuroraPostgresActionConfig auroraPostgresActionConfig) {
    super(auroraPostgresActionConfig, false);
    this.auroraPostgresActionConfig = auroraPostgresActionConfig;
  }

  /**
   * Aurora DB PostgreSQL Action Config.
   */
  public static class AuroraPostgresActionConfig extends DBSpecificQueryConfig {

    @Name(AuroraPostgresConstants.CONNECTION_TIMEOUT)
    @Description(AuroraPostgresConstants.CONNECTION_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String.format(AuroraPostgresConstants.AURORA_POSTGRES_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      if (connectionTimeout != null) {
        return ImmutableMap.of(AuroraPostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
      } else {
        return ImmutableMap.of();
      }
    }
  }
}

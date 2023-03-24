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

package io.cdap.plugin.postgres;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.action.AbstractDBAction;
import io.cdap.plugin.db.config.DBSpecificQueryConfig;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Action that runs PostgreSQL command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Action that runs a PostgreSQL command")
public class PostgresAction extends AbstractDBAction {

  private final PostgresActionConfig postgresActionConfig;

  public PostgresAction(PostgresActionConfig postgresActionConfig) {
    super(postgresActionConfig, false);
    this.postgresActionConfig = postgresActionConfig;
  }

  /**
   * PostgreSQL Action Config.
   */
  public static class PostgresActionConfig extends DBSpecificQueryConfig {

    @Name(PostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String.format(PostgresConstants.POSTGRES_CONNECTION_STRING_WITH_DB_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(PostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }
  }
}

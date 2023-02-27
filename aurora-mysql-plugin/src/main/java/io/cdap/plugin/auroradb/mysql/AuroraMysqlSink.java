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
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.sink.AbstractDBSink;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink support for an Aurora DB MySQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(AuroraMysqlConstants.PLUGIN_NAME)
@Description("Writes records to a table of Aurora DB MySQL cluster. Each record will be written in a row in the table.")
public class AuroraMysqlSink extends AbstractDBSink<AuroraMysqlSink.AuroraMysqlSinkConfig> {

  private final AuroraMysqlSinkConfig auroraMysqlSinkConfig;

  public AuroraMysqlSink(AuroraMysqlSinkConfig auroraMysqlSinkConfig) {
    super(auroraMysqlSinkConfig);
    this.auroraMysqlSinkConfig = auroraMysqlSinkConfig;
  }

  /**
   * Aurora DB MySQL action configuration.
   */
  public static class AuroraMysqlSinkConfig extends DBSpecificSinkConfig {

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

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

package co.cask.auroradb.mysql;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.db.batch.config.DBSpecificSourceConfig;
import co.cask.db.batch.source.AbstractDBSource;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from Aurora DB MySQL type cluster.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(AuroraMysqlConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class AuroraMysqlSource extends AbstractDBSource {

  private final AuroraMysqlSourceConfig auroraMysqlSourceConfig;

  public AuroraMysqlSource(AuroraMysqlSourceConfig auroraMysqlSourceConfig) {
    super(auroraMysqlSourceConfig);
    this.auroraMysqlSourceConfig = auroraMysqlSourceConfig;
  }

  @Override
  protected String createConnectionString(String host, Integer port, String database) {
    return String.format(AuroraMysqlConstants.AURORA_MYSQL_CONNECTION_STRING_FORMAT, host, port, database);
  }

  /**
   * Aurora DB MySQL source config.
   */
  public static class AuroraMysqlSourceConfig extends DBSpecificSourceConfig {

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

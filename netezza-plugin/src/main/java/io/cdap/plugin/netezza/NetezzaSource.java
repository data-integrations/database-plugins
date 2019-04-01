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

package io.cdap.plugin.netezza;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.db.batch.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.apache.sqoop.mapreduce.DBWritable;

/**
 * Batch source to read from Netezza.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(NetezzaConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class NetezzaSource extends AbstractDBSource {

  private final NetezzaSourceConfig netezzaSourceConfig;

  public NetezzaSource(NetezzaSourceConfig netezzaSourceConfig) {
    super(netezzaSourceConfig);
    this.netezzaSourceConfig = netezzaSourceConfig;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return NetezzaDBRecord.class;
  }

  @Override
  protected String createConnectionString(String host, Integer port, String database) {
    return String.format(NetezzaConstants.NETEZZA_CONNECTION_STRING_FORMAT, host, port, database);
  }

  /**
   * Netezza source config.
   */
  public static class NetezzaSourceConfig extends DBSpecificSourceConfig {

    @Override
    public String getConnectionString() {
      return String.format(NetezzaConstants.NETEZZA_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }
}

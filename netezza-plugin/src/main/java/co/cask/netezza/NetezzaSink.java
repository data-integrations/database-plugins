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

import co.cask.DBRecord;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.db.batch.config.DBSpecificSinkConfig;
import co.cask.db.batch.sink.AbstractDBSink;


/**
 * Sink support for a Netezza database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(NetezzaConstants.PLUGIN_NAME)
@Description("Writes records to a Netezza table. Each record will be written in a row in the table")
public class NetezzaSink extends AbstractDBSink {

  private final NetezzaSinkConfig netezzaSinkConfig;

  public NetezzaSink(NetezzaSinkConfig netezzaSinkConfig) {
    super(netezzaSinkConfig);
    this.netezzaSinkConfig = netezzaSinkConfig;
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord.Builder output) {
    return new NetezzaDBRecord(output.build(), columnTypes);
  }

  /**
   * Netezza action configuration.
   */
  public static class NetezzaSinkConfig extends DBSpecificSinkConfig {
    @Override
    public String getConnectionString() {
      return String.format(NetezzaConstants.NETEZZA_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }
}

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

package co.cask.db2;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.db.batch.config.DBSpecificSinkConfig;
import co.cask.db.batch.sink.AbstractDBSink;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;


/**
 * Sink support for a DB2 database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(Db2Constants.PLUGIN_NAME)
@Description("Writes records to a DB2 table. Each record will be written in a row in the table.")
public class Db2Sink extends AbstractDBSink {

  private final Db2SinkConfig db2SinkConfig;

  public Db2Sink(Db2SinkConfig db2SinkConfig) {
    super(db2SinkConfig);
    this.db2SinkConfig = db2SinkConfig;
  }

  /**
   * DB2 action configuration.
   */
  public static class Db2SinkConfig extends DBSpecificSinkConfig {
    @Override
    public String getConnectionString() {
      return String.format(Db2Constants.DB2_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }
}

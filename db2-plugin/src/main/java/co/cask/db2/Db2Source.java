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
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.db.batch.config.DBSpecificSourceConfig;
import co.cask.db.batch.source.AbstractDBSource;
import org.apache.sqoop.mapreduce.DBWritable;

/**
 * Batch source to read from DB2.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(Db2Constants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class Db2Source extends AbstractDBSource {

  private final Db2SourceConfig db2SourceConfig;

  public Db2Source(Db2SourceConfig db2SourceConfig) {
    super(db2SourceConfig);
    this.db2SourceConfig = db2SourceConfig;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return DB2Record.class;
  }

  @Override
  protected String createConnectionString(String host, Integer port, String database) {
    return String.format(Db2Constants.DB2_CONNECTION_STRING_FORMAT, host, port, database);
  }

  /**
   * DB2 source config.
   */
  public static class Db2SourceConfig extends DBSpecificSourceConfig {
    @Override
    public String getConnectionString() {
      return String.format(Db2Constants.DB2_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }
}

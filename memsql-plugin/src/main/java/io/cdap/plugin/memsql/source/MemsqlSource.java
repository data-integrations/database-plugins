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

package io.cdap.plugin.memsql.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.plugin.db.source.AbstractDBSource;
import io.cdap.plugin.memsql.MemsqlConstants;
import io.cdap.plugin.memsql.MemsqlDBRecord;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * Batch source to read from MemSQL.
 */
@Plugin(type = "batchsource")
@Name(MemsqlConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class MemsqlSource extends AbstractDBSource<MemsqlSourceConfig> {

  private final MemsqlSourceConfig memsqlSourceConfig;

  public MemsqlSource(MemsqlSourceConfig memsqlSourceConfig) {
    super(memsqlSourceConfig);
    this.memsqlSourceConfig = memsqlSourceConfig;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return MemsqlDBRecord.class;
  }

  @Override
  protected String createConnectionString() {
    return String.format(MemsqlConstants.MEMSQL_CONNECTION_STRING_FORMAT,
                         memsqlSourceConfig.host, memsqlSourceConfig.port, memsqlSourceConfig.database);
  }
}

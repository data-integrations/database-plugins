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

package io.cdap.plugin.teradata.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import io.cdap.plugin.teradata.TeradataConstants;
import io.cdap.plugin.teradata.TeradataDBRecord;
import io.cdap.plugin.teradata.TeradataSchemaReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * Batch source to read from Teradata.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(TeradataConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class TeradataSource extends AbstractDBSource {
  private final TeradataSourceConfig config;

  public TeradataSource(TeradataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected Class<? extends DBWritable> getDBRecordType() {
    return TeradataDBRecord.class;
  }

  @Override
  protected String createConnectionString() {
    return config.getConnectionString();
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new TeradataSchemaReader();
  }
}

/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.teradata.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import io.cdap.plugin.db.batch.sink.FieldsValidator;
import io.cdap.plugin.teradata.TeradataConstants;
import io.cdap.plugin.teradata.TeradataDBRecord;
import io.cdap.plugin.teradata.TeradataSchemaReader;

/**
 * Sink support for a Teradata database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(TeradataConstants.PLUGIN_NAME)
@Description("Writes records to a Teradata table. Each record will be written in a row in the table")
public class TeradataSink  extends AbstractDBSink {
  private final TeradataSinkConfig config;

  public TeradataSink(TeradataSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new TeradataSchemaReader();
  }

  @Override
  protected FieldsValidator getFieldsValidator() {
    return new TeradataFieldsValidator();
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new TeradataDBRecord(output, columnTypes);
  }
}

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

package io.cdap.plugin.oracle;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.SchemaReader;

import java.util.List;

/**
 * Oracle Sink implementation {@link org.apache.hadoop.mapreduce.lib.db.DBWritable} and
 * {@link org.apache.hadoop.io.Writable}.
 */
public class OracleSinkDBRecord extends OracleSourceDBRecord {

  public OracleSinkDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSinkSchemaReader();
  }
}

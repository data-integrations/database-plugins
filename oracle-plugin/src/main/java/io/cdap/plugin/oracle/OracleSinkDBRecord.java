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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.Operation;
import io.cdap.plugin.db.SchemaReader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Oracle Sink implementation {@link org.apache.hadoop.mapreduce.lib.db.DBWritable} and
 * {@link org.apache.hadoop.io.Writable}.
 */
public class OracleSinkDBRecord extends OracleSourceDBRecord {

  public OracleSinkDBRecord(StructuredRecord record, List<ColumnType> columnTypes, Operation operationName,
      String relationTableKey) {
    this.record = record;
    this.columnTypes = columnTypes;
    this.operationName = operationName;
    this.relationTableKey = relationTableKey;
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSinkSchemaReader();
  }

  @Override
  protected void insertOperation(PreparedStatement stmt) throws SQLException {
    for (int fieldIndex = 0; fieldIndex < columnTypes.size(); fieldIndex++) {
      ColumnType columnType = columnTypes.get(fieldIndex);
      // Get the field from the schema using the column name with ignoring case.
      Schema.Field field = record.getSchema().getField(columnType.getName(), true);
      writeToDB(stmt, field, fieldIndex);
    }
  }

  @Override
  protected void upsertOperation(PreparedStatement stmt) throws SQLException {
    for (int fieldIndex = 0; fieldIndex < columnTypes.size(); fieldIndex++) {
      ColumnType columnType = columnTypes.get(fieldIndex);
      Schema.Field field = record.getSchema().getField(columnType.getName());
      writeToDB(stmt, field, fieldIndex);
    }
  }
}

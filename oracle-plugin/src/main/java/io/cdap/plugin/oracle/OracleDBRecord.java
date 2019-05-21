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
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Writable class for Oracle Source/Sink
 */
public class OracleDBRecord extends DBRecord {

  public OracleDBRecord(StructuredRecord record, int[] columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public OracleDBRecord() {
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSchemaReader();
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (OracleSchemaReader.ORACLE_TYPES.contains(sqlType)) {
      handleOracleSpecificType(resultSet, recordBuilder, field, columnIndex, sqlType);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  private void handleOracleSpecificType(ResultSet resultSet,
                                        StructuredRecord.Builder recordBuilder, Schema.Field field,
                                        int columnIndex, int sqlType) throws SQLException {

    switch (sqlType) {
      case OracleSchemaReader.INTERVAL_YM:
      case OracleSchemaReader.INTERVAL_DS:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
      case OracleSchemaReader.TIMESTAMP_LTZ:
      case OracleSchemaReader.TIMESTAMP_TZ:
        Instant instant = resultSet.getTimestamp(columnIndex).toInstant();
        recordBuilder.setTimestamp(field.getName(), instant.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
        break;
    }
  }
}

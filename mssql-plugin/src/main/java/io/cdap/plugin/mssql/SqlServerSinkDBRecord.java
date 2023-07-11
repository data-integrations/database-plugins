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

package io.cdap.plugin.mssql;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.SchemaReader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * SQL Server Sink implementation {@link org.apache.hadoop.mapreduce.lib.db.DBWritable} and
 * {@link org.apache.hadoop.io.Writable}.
 */
public class SqlServerSinkDBRecord extends SqlServerSourceDBRecord {

  public SqlServerSinkDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  @Override
  protected void writeNullToDB(PreparedStatement stmt, int fieldIndex) throws SQLException {
    int sqlType = columnTypes.get(fieldIndex).getType();
    int sqlIndex = fieldIndex + 1;
    if (sqlType == SqlServerSourceSchemaReader.GEOGRAPHY_TYPE
        || sqlType == SqlServerSourceSchemaReader.GEOMETRY_TYPE) {
      // Handle setting GEOGRAPHY and GEOMETRY 'null' values. Using 'stmt.setNull(sqlIndex, GEOMETRY_TYPE)' leads
      // to com.microsoft.sqlserver.jdbc.SQLServerException: The conversion from OBJECT to GEOMETRY is unsupported
      stmt.setString(sqlIndex, "Null");
      return;
    }
    super.writeNullToDB(stmt, fieldIndex);
  }

  @Override
  protected void writeNonNullToDB(PreparedStatement stmt, Schema fieldSchema,
                                  String fieldName, int fieldIndex) throws SQLException {
    int sqlType = columnTypes.get(fieldIndex).getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();
    int sqlIndex = fieldIndex + 1;
    if (fieldLogicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
      ZonedDateTime timestamp = record.getTimestamp(fieldName);
      if (timestamp != null) {
        Timestamp localTimestamp = Timestamp.valueOf(timestamp.toLocalDateTime());
        stmt.setTimestamp(sqlIndex, localTimestamp);
      } else {
        stmt.setNull(sqlIndex, sqlType);
      }
      return;
    }
    switch (sqlType) {
      case SqlServerSourceSchemaReader.GEOGRAPHY_TYPE:
      case SqlServerSourceSchemaReader.GEOMETRY_TYPE:
        Object fieldValue = (fieldName != null) ? record.get(fieldName) : null;
        if (fieldValue instanceof String) {
          // Handle setting GEOGRAPHY and GEOMETRY values from Well Known Text.
          // For example, "POINT(3 40 5 6)"
          stmt.setString(sqlIndex, (String) fieldValue);
        } else {
          super.writeBytes(stmt, fieldIndex, sqlIndex, fieldValue);
        }
        break;
      case Types.TIME:
        // Handle setting SQL Server 'TIME' data type as string to avoid accuracy loss. 'TIME' data type has the
        // accuracy of 100 nanoseconds(1 millisecond in Informatica) but 'java.sql.Time' will round value to second.
        stmt.setString(sqlIndex, record.getTime(fieldName).toString());
        break;
      default:
        super.writeNonNullToDB(stmt, fieldSchema, fieldName, fieldIndex);
    }
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSinkSchemaReader();
  }
}

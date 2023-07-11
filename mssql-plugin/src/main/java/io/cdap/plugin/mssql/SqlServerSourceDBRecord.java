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
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 * SQL Server Source implementation {@link org.apache.hadoop.mapreduce.lib.db.DBWritable} and {@link
 * org.apache.hadoop.io.Writable}.
 */
public class SqlServerSourceDBRecord extends DBRecord {

  public SqlServerSourceDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  public SqlServerSourceDBRecord() {
    // Required by Hadoop DBRecordReader to create an instance
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder,
      Schema.Field field, int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    Schema fieldSchema = field.getSchema();
    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    switch (sqlType) {
      case Types.TIMESTAMP:
        // SmallDateTime, DateTime, DateTime2 usecase
        GregorianCalendar gc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        gc.setGregorianChange(new Date(Long.MIN_VALUE));
        Timestamp timestampSmalldatetime = resultSet.getTimestamp(columnIndex, gc);
        if (timestampSmalldatetime == null) {
          recordBuilder.set(field.getName(), null);
        } else if (fieldSchema.getLogicalType() == Schema.LogicalType.DATETIME) {
          // SmallDateTime, Datetime, datetime2 to CDAP Datetime type conversion
          setDateTime(resultSet, recordBuilder, field, columnIndex);
        } else {
            // Deprecated use case of supporting SmallDateTime to CDAP Timestamp conversion
            recordBuilder.setTimestamp(field.getName(), timestampSmalldatetime.toInstant()
              .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
        }
        break;
      case SqlServerSourceSchemaReader.DATETIME_OFFSET_TYPE:
        OffsetDateTime timestampOffset = resultSet.getObject(columnIndex, OffsetDateTime.class);
        if (timestampOffset == null) {
          recordBuilder.set(field.getName(), null);
        } else if (fieldSchema.getLogicalType() == Schema.LogicalType.TIMESTAMP_MICROS) {
          // DateTimeOffset to CDAP Timestamp type conversion
          recordBuilder.setTimestamp(field.getName(), timestampOffset.atZoneSameInstant(ZoneId.of("UTC")));
        } else {
          // Deprecated use case of supporting DateTimeOffset to CDAP DateTime conversion.
          setDateTime(resultSet, recordBuilder, field, columnIndex);
        }
        break;
      case Types.TIME:
        // Handle reading SQL Server 'TIME' data type to avoid accuracy loss.
        // 'TIME' data type has the accuracy of 100 nanoseconds(1 millisecond in Informatica)
        // but reading via 'getTime' and 'getObject' will round value to second.
        final Timestamp timestamp = resultSet.getTimestamp(columnIndex);
        recordBuilder.setTime(field.getName(),
            timestamp == null ? null : timestamp.toLocalDateTime().toLocalTime());
        break;
      default:
        setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  public void setDateTime(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                          int columnIndex) throws SQLException {
    try {
      Method getLocalDateTime = resultSet.getClass().getMethod("getDateTime", int.class);
      Timestamp value = (Timestamp) getLocalDateTime.invoke(resultSet, columnIndex);
      recordBuilder.setDateTime(field.getName(), value == null ? null : value.toLocalDateTime());
    } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(String.format("Fail to convert column %s of type %s to datetime. Error: %s.",
                                               resultSet.getMetaData().getColumnName(columnIndex),
                                               resultSet.getMetaData().getColumnTypeName(columnIndex),
                                               e.getMessage()), e);
    }
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSourceSchemaReader();
  }
}

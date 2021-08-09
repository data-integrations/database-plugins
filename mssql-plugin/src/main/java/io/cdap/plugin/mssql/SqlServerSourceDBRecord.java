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
import java.util.List;

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
                             Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (SqlServerSourceSchemaReader.shouldConvertToDatetime(resultSet.getMetaData(), columnIndex)) {
      try {
        Method getLocalDateTime = resultSet.getClass().getMethod("getDateTime", int.class);
        Timestamp value = (Timestamp) getLocalDateTime.invoke(resultSet, columnIndex);
        recordBuilder.setDateTime(field.getName(), value == null ? null : value.toLocalDateTime());
        return;
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException(String.format("Fail to convert column %s of type %s to datetime. Error: %s.",
                                                 resultSet.getMetaData().getColumnName(columnIndex),
                                                 resultSet.getMetaData().getColumnTypeName(columnIndex),
                                                 e.getMessage()), e);
      }
    }
    switch (sqlType) {
      case Types.TIME:
        // Handle reading SQL Server 'TIME' data type to avoid accuracy loss.
        // 'TIME' data type has the accuracy of 100 nanoseconds(1 millisecond in Informatica)
        // but reading via 'getTime' and 'getObject' will round value to second.
        final Timestamp timestamp = resultSet.getTimestamp(columnIndex);
        recordBuilder.setTime(field.getName(),
            timestamp == null ? null : timestamp.toLocalDateTime().toLocalTime());
        break;
      case SqlServerSourceSchemaReader.DATETIME_OFFSET_TYPE:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
      default:
        setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSourceSchemaReader();
  }
}

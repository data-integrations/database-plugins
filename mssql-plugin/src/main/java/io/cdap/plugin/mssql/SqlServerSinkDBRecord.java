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
import java.sql.Types;
import java.util.List;
import javax.annotation.Nullable;

/**
 * SQL Server Sink implementation {@link org.apache.hadoop.mapreduce.lib.db.DBWritable} and
 * {@link org.apache.hadoop.io.Writable}.
 */
public class SqlServerSinkDBRecord extends SqlServerSourceDBRecord {

  public SqlServerSinkDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  @Override
  protected void writeToDB(PreparedStatement stmt, @Nullable Schema.Field field, int fieldIndex) throws SQLException {
    Object fieldValue = (field != null) ? record.get(field.getName()) : null;
    int sqlType = columnTypes.get(fieldIndex).getType();
    int sqlIndex = fieldIndex + 1;
    switch (sqlType) {
      case SqlServerSourceSchemaReader.GEOGRAPHY_TYPE:
      case SqlServerSourceSchemaReader.GEOMETRY_TYPE:
        if (fieldValue == null) {
          // Handle setting GEOGRAPHY and GEOMETRY 'null' values. Using 'stmt.setNull(sqlIndex, GEOMETRY_TYPE)' leads
          // to com.microsoft.sqlserver.jdbc.SQLServerException: The conversion from OBJECT to GEOMETRY is unsupported
          stmt.setString(sqlIndex, "Null");
        } else if (fieldValue instanceof String) {
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
        if (fieldValue != null) {
          String fieldName = field.getName();
          stmt.setString(sqlIndex, record.getTime(fieldName).toString());
        } else {
          stmt.setNull(sqlIndex, sqlType);
        }
        break;
      default:
        super.writeToDB(stmt, field, fieldIndex);
    }
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new SqlServerSinkSchemaReader();
  }
}

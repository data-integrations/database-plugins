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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * SQL Server Source schema reader.
 */
public class SqlServerSourceSchemaReader extends CommonSchemaReader {

  public static final String TIMESTAMP_TYPE_NAME = "TIMESTAMP";
  public static final int DATETIME_OFFSET_TYPE = -155;
  public static final int GEOMETRY_TYPE = -157;
  public static final int GEOGRAPHY_TYPE = -158;
  public static final int SQL_VARIANT = -156;
  public static final String DATETIME_TYPE_PREFIX = "datetime";

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int columnSqlType = metadata.getColumnType(index);

    if (shouldConvertToDatetime(metadata, index)) {
      return Schema.of(Schema.LogicalType.DATETIME);

    }
    if (GEOMETRY_TYPE == columnSqlType || GEOGRAPHY_TYPE == columnSqlType) {
      return Schema.of(Schema.Type.BYTES);
    }
    return super.getSchema(metadata, index);
  }

  /**
   * Whether the corresponding column should be converted to CDAP Datetime Logical Type
   * @param metadata result set metadata
   * @param index index of the column
   * @return whether the corresponding column should be converted to CDAP Datetime Logical Type
   * @throws SQLException
   */
  public static boolean shouldConvertToDatetime(ResultSetMetaData metadata, int index) throws SQLException {
    // datetimeoffset will have type DATETIME_OFFSET_TYPE
    // datetime and datetime2 will have type Types.TIMESTAMP
    // cannot decide based on sql type
    String columnTypeName = metadata.getColumnTypeName(index);
    return columnTypeName.startsWith(DATETIME_TYPE_PREFIX);
  }
}

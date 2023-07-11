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
import java.sql.Types;

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

  private final String sessionID;

  public SqlServerSourceSchemaReader() {
    this(null);
  }

  public SqlServerSourceSchemaReader(String sessionID) {
    super();
    this.sessionID = sessionID;
  }

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int columnSqlType = metadata.getColumnType(index);

    if (DATETIME_OFFSET_TYPE == columnSqlType) {
      return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
    }
    if (columnSqlType == Types.TIMESTAMP) {
      // SmallDateTime, DateTime and DateTime2 will be mapped to Types.DATETIME
      return Schema.of(Schema.LogicalType.DATETIME);
    }
    if (GEOMETRY_TYPE == columnSqlType || GEOGRAPHY_TYPE == columnSqlType) {
      return Schema.of(Schema.Type.BYTES);
    }
    return super.getSchema(metadata, index);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    if (sessionID == null) {
      return false;
    }
    return metadata.getColumnName(index).equals("c_" + sessionID) ||
      metadata.getColumnName(index).equals("sqn_" + sessionID);
  }
}

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
import io.cdap.plugin.db.sink.CommonFieldsValidator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * MSSQL validator for DB fields.
 */
public class SqlFieldsValidator extends CommonFieldsValidator {

  @Override
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    int sqlType = metadata.getColumnType(index);
    boolean isSigned = metadata.isSigned(index);
    int precision = metadata.getPrecision(index);

    // Handle logical types first
    if (fieldLogicalType != null) {
      if ((sqlType == Types.TIMESTAMP || sqlType == SqlServerSourceSchemaReader.DATETIME_OFFSET_TYPE)
              && fieldLogicalType.equals(Schema.LogicalType.DATETIME)) {
        return true;
      }
      return super.isFieldCompatible(fieldType, fieldLogicalType, sqlType, precision, isSigned);
    }

    switch (fieldType) {
      case BYTES:
        return sqlType == SqlServerSinkSchemaReader.GEOGRAPHY_TYPE
          || sqlType == SqlServerSinkSchemaReader.GEOMETRY_TYPE
          || super.isFieldCompatible(field, metadata, index);
      case STRING:
        return sqlType == SqlServerSinkSchemaReader.DATETIME_OFFSET_TYPE
          // Value of GEOMETRY and GEOGRAPHY type can be set as Well Known Text string such as "POINT(3 40 5 6)"
          || sqlType == SqlServerSinkSchemaReader.GEOGRAPHY_TYPE
          || sqlType == SqlServerSinkSchemaReader.GEOMETRY_TYPE
          || sqlType == SqlServerSinkSchemaReader.SQL_VARIANT
          || sqlType == Types.ROWID
          || sqlType == Types.NUMERIC
          || super.isFieldCompatible(field, metadata, index);
      default:
        return super.isFieldCompatible(fieldType, null, sqlType, precision, isSigned);
    }
  }
}

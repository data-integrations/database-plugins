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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.batch.sink.CommonFieldsValidator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Oracle validator for DB fields.
 */
public class OracleFieldsValidator extends CommonFieldsValidator {

  @Override
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    int sqlType = metadata.getColumnType(index);
    int precision = metadata.getPrecision(index);
    int scale = metadata.getScale(index);

    // Handle logical types first
    if (fieldLogicalType == Schema.LogicalType.DATE) {
      // oracle date also contains time part so the oracle driver treats date as timestamp
      // https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT413
      return sqlType == Types.TIMESTAMP || super.isFieldCompatible(fieldType, fieldLogicalType, sqlType);
    }
    if (fieldLogicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
      return sqlType == OracleSinkSchemaReader.TIMESTAMP_LTZ ||
        super.isFieldCompatible(fieldType, fieldLogicalType, sqlType);
    } else if (fieldLogicalType != null) {
      return super.isFieldCompatible(fieldType, fieldLogicalType, sqlType);
    }

    switch (fieldType) {
      case FLOAT:
        return sqlType == OracleSinkSchemaReader.BINARY_FLOAT
          || super.isFieldCompatible(fieldType, null, sqlType);
      case BYTES:
        return sqlType == OracleSinkSchemaReader.BFILE
          || sqlType == OracleSinkSchemaReader.LONG_RAW
          || super.isFieldCompatible(fieldType, null, sqlType);
      case STRING:
        return sqlType == OracleSinkSchemaReader.LONG
          || sqlType == OracleSinkSchemaReader.TIMESTAMP_TZ
          || sqlType == OracleSinkSchemaReader.INTERVAL_DS
          || sqlType == OracleSinkSchemaReader.INTERVAL_YM
          || sqlType == Types.ROWID
          || super.isFieldCompatible(fieldType, null, sqlType);
      case INT:
        // Since all Oracle numeric types are based on NUMBER(i.e. INTEGER type is actually NUMBER(38, 0)) we won't be
        // able to use Oracle Sink Plugin with other sources. It's safe to write primitives as values of decimal
        // logical type in the case of valid precision.
        // The following schema compatibility is supported:
        // 1) Schema.Type.INT -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 10)
        if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC) {
          // With 10 digits we can represent Integer.MAX_VALUE.
          // It is equal to the value returned by (new BigDecimal(Integer.MAX_VALUE)).precision()
          return precision >= 10 && scale == 0;
        }
        return super.isFieldCompatible(fieldType, null, sqlType);
      case LONG:
        // Since all Oracle numeric types are based on NUMBER(i.e. INTEGER type is actually NUMBER(38, 0)) we won't be
        // able to use Oracle Sink Plugin with other sources. It's safe to write primitives as values of decimal
        // logical type in the case of valid precision.
        // The following schema compatibility is supported:
        // 2) Schema.Type.LONG -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 19)
        if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC) {
          // With 19 digits we can represent Long.MAX_VALUE.
          // It is equal to the value returned by (new BigDecimal(Long.MAX_VALUE)).precision()
          return precision >= 19 && scale == 0;
        }
        return super.isFieldCompatible(fieldType, null, sqlType);
      case DOUBLE:
        if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC) {
          // This is the only way to differentiate FLOAT/REAL columns from other numeric columns, that based on NUMBER.
          // Since in Oracle FLOAT is a subtype of the NUMBER data type, 'getColumnType' and 'getColumnTypeName' can not
          // be used.
          return Double.class.getTypeName().equals(metadata.getColumnClassName(index));
        }
        return sqlType == OracleSinkSchemaReader.BINARY_DOUBLE
          || super.isFieldCompatible(fieldType, null, sqlType);
      default:
        return super.isFieldCompatible(fieldType, null, sqlType);
    }
  }
}

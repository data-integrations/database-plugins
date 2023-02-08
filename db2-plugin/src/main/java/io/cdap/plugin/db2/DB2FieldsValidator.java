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

package io.cdap.plugin.db2;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.sink.CommonFieldsValidator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * DB2 validator for DB fields.
 */
public class DB2FieldsValidator extends CommonFieldsValidator {

  @Override
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    int sqlType = metadata.getColumnType(index);
    String colTypeName = metadata.getColumnTypeName(index);
    boolean isSigned = metadata.isSigned(index);
    int precision = metadata.getPrecision(index);

    // Handle logical types first
    if (fieldLogicalType != null) {
      return super.isFieldCompatible(fieldType, fieldLogicalType, sqlType, precision, isSigned);
    }

    switch (fieldType) {
      case STRING:
        return sqlType == Types.OTHER
          //DECFLOAT is mapped to string
          || DB2SchemaReader.DB2_DECFLOAT.equals(colTypeName)
          || super.isFieldCompatible(field, metadata, index);
      default:
        return super.isFieldCompatible(fieldType, null, sqlType, precision, isSigned);
    }
  }
}

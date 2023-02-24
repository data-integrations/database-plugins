/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.teradata.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.sink.CommonFieldsValidator;
import io.cdap.plugin.teradata.TeradataSchemaReader;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Teradata validator for DB fields.
 */
public class TeradataFieldsValidator extends CommonFieldsValidator {

  @Override
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    int sqlType = metadata.getColumnType(index);
    String sqlTypeName = metadata.getColumnTypeName(index);

    // In Teradata FLOAT and DOUBLE are same types
    if (fieldType == Schema.Type.DOUBLE && sqlType == Types.FLOAT) {
      return true;
    }

    // Teradata interval types are mapping to String
    if (fieldType == Schema.Type.STRING && TeradataSchemaReader.TERADATA_STRING_TYPES.contains(sqlTypeName)) {
      return true;
    }

    return isFieldCompatible(fieldType, fieldLogicalType, sqlType);
  }
}

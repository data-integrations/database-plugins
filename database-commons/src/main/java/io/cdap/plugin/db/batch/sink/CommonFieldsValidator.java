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

package io.cdap.plugin.db.batch.sink;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

/**
 * Common fields validator.
 */
public class CommonFieldsValidator implements FieldsValidator {
  protected static final Logger LOG = LoggerFactory.getLogger(CommonFieldsValidator.class);

  @Override
  public void validateFields(Schema inputSchema, ResultSet resultSet) throws SQLException {
    ResultSetMetaData rsMetaData = resultSet.getMetaData();

    Preconditions.checkNotNull(inputSchema.getFields());
    Set<String> invalidFields = new HashSet<>();
    for (Schema.Field field : inputSchema.getFields()) {
      int columnIndex = resultSet.findColumn(field.getName());
      boolean isColumnNullable = (ResultSetMetaData.columnNullable == rsMetaData.isNullable(columnIndex));
      boolean isNotNullAssignable = !isColumnNullable && field.getSchema().isNullable();
      if (isNotNullAssignable) {
        LOG.error("Field '{}' was given as nullable but the database column is not nullable", field.getName());
        invalidFields.add(field.getName());
      }

      if (!isFieldCompatible(field, rsMetaData, columnIndex)) {
        String sqlTypeName = rsMetaData.getColumnTypeName(columnIndex);
        Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
        Schema.Type fieldType = fieldSchema.getType();
        Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();
        LOG.error("Field '{}' was given as type '{}' but the database column is actually of type '{}'.",
                  field.getName(),
                  fieldLogicalType != null ? fieldLogicalType.getToken() : fieldType,
                  sqlTypeName
        );
        invalidFields.add(field.getName());
      }
    }

    Preconditions.checkArgument(invalidFields.isEmpty(),
                                "Couldn't find matching database column(s) for input field(s) '%s'.",
                                String.join(",", invalidFields));
  }

  /**
   * Checks if field is compatible to be written into database column of the given sql index.
   *
   * @param field    field of the explicit input schema.
   * @param metadata resultSet metadata.
   * @param index    sql column index.
   * @return 'true' if field is compatible to be written, 'false' otherwise.
   */
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    int sqlType = metadata.getColumnType(index);

    return isFieldCompatible(fieldType, fieldLogicalType, sqlType);
  }


  /**
   * Checks if field is compatible to be written into database column of the given sql index.
   *
   * @param fieldType        field type.
   * @param fieldLogicalType filed logical type.
   * @param sqlType          code of sql type.
   * @return 'true' if field is compatible to be written, 'false' otherwise.
   */
  public boolean isFieldCompatible(Schema.Type fieldType, Schema.LogicalType fieldLogicalType, int sqlType) {
    // Handle logical types first
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DATE:
          return sqlType == Types.DATE;
        case TIME_MILLIS:
        case TIME_MICROS:
          return sqlType == Types.TIME;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return sqlType == Types.TIMESTAMP;
        case DECIMAL:
          return sqlType == Types.NUMERIC
            || sqlType == Types.DECIMAL;
      }
    }

    switch (fieldType) {
      case NULL:
        return true;
      case BOOLEAN:
        return sqlType == Types.BOOLEAN
          || sqlType == Types.BIT;
      case INT:
        return sqlType == Types.INTEGER
          || sqlType == Types.SMALLINT
          || sqlType == Types.TINYINT;
      case LONG:
        return sqlType == Types.BIGINT;
      case FLOAT:
        return sqlType == Types.REAL
          || sqlType == Types.FLOAT;
      case DOUBLE:
        return sqlType == Types.DOUBLE;
      case BYTES:
        return sqlType == Types.BINARY
          || sqlType == Types.VARBINARY
          || sqlType == Types.LONGVARBINARY
          || sqlType == Types.BLOB;
      case STRING:
        return sqlType == Types.VARCHAR
          || sqlType == Types.CHAR
          || sqlType == Types.CLOB
          || sqlType == Types.LONGNVARCHAR
          || sqlType == Types.LONGVARCHAR
          || sqlType == Types.NCHAR
          || sqlType == Types.NCLOB
          || sqlType == Types.NVARCHAR;
      default:
        return false;
    }
  }
}

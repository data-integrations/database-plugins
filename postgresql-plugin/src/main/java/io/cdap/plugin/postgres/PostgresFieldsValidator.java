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

package io.cdap.plugin.postgres;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.sink.CommonFieldsValidator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

/**
 * Postgres validator for DB fields.
 */
public class PostgresFieldsValidator extends CommonFieldsValidator {

  @Override
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema schema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = schema.getType();

    String colTypeName = metadata.getColumnTypeName(index);
    int columnType = metadata.getColumnType(index);
    if (PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES_NAMES.contains(colTypeName) ||
      PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES.contains(columnType)) {
      if (Objects.equals(fieldType, Schema.Type.STRING)) {
        return true;
      } else {
        LOG.error("Field '{}' was given as type '{}' but must be of type 'string' for the PostgreSQL column of " +
                    "{} type.", field.getName(), fieldType, colTypeName);
        return false;
      }
    }

    // Since Numeric types without precision and scale are getting converted into CDAP String type at the Source
    // plugin, hence making the String type compatible with the Numeric type at the Sink as well.
    if (fieldType.equals(Schema.Type.STRING)) {
      if (Types.NUMERIC == columnType) {
        return true;
      }
    }
    
    if (colTypeName.equalsIgnoreCase("timestamp")
        && schema.getLogicalType().equals(Schema.LogicalType.DATETIME)) {
      return true;
    }

    return super.isFieldCompatible(field, metadata, index);
  }
}

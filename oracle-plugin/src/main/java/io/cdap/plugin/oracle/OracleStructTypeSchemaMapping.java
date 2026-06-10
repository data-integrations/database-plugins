/*
 * Copyright © 2026 Cask Data, Inc.
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
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry containing schema type mappers for Oracle specific datatypes.
 */
public final class OracleStructTypeSchemaMapping {
  private static final Logger LOG = LoggerFactory.getLogger(OracleStructTypeSchemaMapping.class);

  private interface TypeMapper {
    Schema map(boolean isTimestampOldBehavior, Schema timestampLtzSchema,
               boolean isPrecisionlessNumAsDecimal, String typeName, int precision, int scale, String columnName);
  }

  private static final Map<String, TypeMapper> TYPE_MAPPERS = new HashMap<>();

  static {
    TypeMapper floatMapper = (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.Type.FLOAT);
    TYPE_MAPPERS.put("BINARY_FLOAT", floatMapper);
    TYPE_MAPPERS.put("REAL", floatMapper);
    TYPE_MAPPERS.put("FLOAT", floatMapper);

    TypeMapper doubleMapper = (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.Type.DOUBLE);
    TYPE_MAPPERS.put("BINARY_DOUBLE", doubleMapper);
    TYPE_MAPPERS.put("DOUBLE", doubleMapper);

    // Bytes types
    TypeMapper bytesMapper = (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.Type.BYTES);
    TYPE_MAPPERS.put("BFILE", bytesMapper);
    TYPE_MAPPERS.put("BLOB", bytesMapper);
    TYPE_MAPPERS.put("RAW", bytesMapper);
    TYPE_MAPPERS.put("LONG RAW", bytesMapper);

    // String types
    TypeMapper stringMapper = (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.Type.STRING);
    TYPE_MAPPERS.put("INTERVAL DAY TO SECOND", stringMapper);
    TYPE_MAPPERS.put("INTERVAL YEAR TO MONTH", stringMapper);
    TYPE_MAPPERS.put("VARCHAR2", stringMapper);
    TYPE_MAPPERS.put("VARCHAR", stringMapper);
    TYPE_MAPPERS.put("CHAR", stringMapper);
    TYPE_MAPPERS.put("CHAR2", stringMapper);
    TYPE_MAPPERS.put("NCHAR", stringMapper);
    TYPE_MAPPERS.put("NVARCHAR2", stringMapper);
    TYPE_MAPPERS.put("CLOB", stringMapper);
    TYPE_MAPPERS.put("NCLOB", stringMapper);
    TYPE_MAPPERS.put("LONG", stringMapper);
    TYPE_MAPPERS.put("ROWID", stringMapper);
    TYPE_MAPPERS.put("UROWID", stringMapper);

    // Date and Time types
    TYPE_MAPPERS.put("TIMESTAMP WITH TZ", (isOld, ltzS, precD, typeName, p, s, col) ->
            isOld ? Schema.of(Schema.Type.STRING) : Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)
    );
    TYPE_MAPPERS.put("TIMESTAMP WITH LOCAL TZ", (isOld, ltzS, precD, typeName, p, s, col) -> ltzS);
    TYPE_MAPPERS.put("TIMESTAMP", (isOld, ltzS, precD, typeName, p, s, col) ->
            isOld ? Schema.of(Schema.LogicalType.TIMESTAMP_MICROS) : Schema.of(Schema.LogicalType.DATETIME)
    );
    TYPE_MAPPERS.put("DATE", (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.LogicalType.DATE));
    TYPE_MAPPERS.put("TIME", (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.LogicalType.TIME_MICROS));

    // Numeric types
    TYPE_MAPPERS.put("INTEGER", (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.Type.INT));
    TYPE_MAPPERS.put("NUMBER", OracleStructTypeSchemaMapping::mapNumberOrDecimal);
    TYPE_MAPPERS.put("DECIMAL", OracleStructTypeSchemaMapping::mapNumberOrDecimal);

    // XML type
    TYPE_MAPPERS.put("XMLTYPE", (isOld, ltzS, precD, typeName, p, s, col) -> Schema.of(Schema.Type.STRING));

    // Unsupported types that throw error
    TYPE_MAPPERS.put("ARRAY", (isOld, ltzS, precD, typeName, p, s, col) -> {
      String errorMessage = String.format("Column %s has unsupported SQL type of %s.", col, typeName);
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
              errorMessage, errorMessage, ErrorType.SYSTEM, true, null);
    });
    TYPE_MAPPERS.put("ANYDATA", (isOld, ltzS, precD, typeName, p, s, col) -> {
      String errorMessage = String.format("Column %s has unsupported SQL type of %s.", col, typeName);
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
              errorMessage, errorMessage, ErrorType.SYSTEM, true, null);
    });
    TYPE_MAPPERS.put("OTHER", (isOld, ltzS, precD, typeName, p, s, col) -> {
      String errorMessage = String.format("Column %s has unsupported SQL type of %s.", col, typeName);
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
              errorMessage, errorMessage, ErrorType.SYSTEM, true, null);
    });
  }

  private OracleStructTypeSchemaMapping() {
    // Private constructor to prevent instantiation of utility class.
  }

  /**
   * Maps primitive Oracle types to CDAP Schemas.
   */
  public static Schema mapPrimitiveOracleType(boolean isTimestampOldBehavior, Schema timestampLtzSchema,
                                              boolean isPrecisionlessNumAsDecimal, String typeName,
                                              int precision, int scale, String columnName) {
    TypeMapper mapper = TYPE_MAPPERS.get(typeName);
    if (mapper != null) {
      return mapper.map(isTimestampOldBehavior, timestampLtzSchema, isPrecisionlessNumAsDecimal,
              typeName, precision, scale, columnName);
    }
    return null;
  }

  private static Schema mapNumberOrDecimal(boolean isOld, Schema ltzS, boolean precD, String typeName,
                                           int precision, int scale, String columnName) {
    if (Double.class.getTypeName().equals(typeName)) {
      return Schema.of(Schema.Type.DOUBLE);
    } else {
      if (precision == 0) {
        if (precD) {
          int newPrecision = 38;
          int newScale = 0;
          LOG.warn(String.format("%s type with undefined precision and scale is detected, "
                  + "there may be a precision loss while running the pipeline. "
                  + "Please define an output precision and scale for field to avoid "
                  + "precision loss.", typeName));
          return Schema.decimalOf(newPrecision, newScale);
        } else {
          LOG.warn(String.format("%s type without precision and scale, "
                          + "converting into STRING type to avoid any precision loss.",
                  typeName));
          return Schema.of(Schema.Type.STRING);
        }
      }
      return Schema.decimalOf(precision, scale);
    }
  }
}

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

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * PostgreSQL schema reader.
 */
public class PostgresSchemaReader extends CommonSchemaReader {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaReader.class);

  public static final Set<Integer> STRING_MAPPED_POSTGRES_TYPES = ImmutableSet.of(
    Types.OTHER, Types.ARRAY, Types.SQLXML
  );

  public static final Set<String> STRING_MAPPED_POSTGRES_TYPES_NAMES = ImmutableSet.of(
    "bit", "timetz", "money"
  );

  private final String sessionID;

  public PostgresSchemaReader() {
    this(null);
  }

  public PostgresSchemaReader(String sessionID) {
    super();
    this.sessionID = sessionID;
  }

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    String typeName = metadata.getColumnTypeName(index);
    int columnType = metadata.getColumnType(index);
    int precision = metadata.getPrecision(index);
    String columnName = metadata.getColumnName(index);
    int scale = metadata.getScale(index);
    boolean isSigned = metadata.isSigned(index);

    return getSchema(typeName, columnType, precision, scale, columnName, isSigned, true);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    if (sessionID == null) {
      return false;
    }
    return metadata.getColumnName(index).equals("c_" + sessionID) ||
      metadata.getColumnName(index).equals("sqn_" + sessionID);
  }

  /**
   * Returns the CDAP schema for a PostgreSQL column, handling special cases for certain types.
   * Maps PostgreSQL-specific types (like bit, timetz, money, arrays) to STRING, handles
   * INT and BIGINT directly, and maps numeric/decimal types with zero precision to STRING
   * to avoid precision loss. Timestamps are mapped to DATETIME. Falls back to DBUtils for others.
   *
   * @param typeName        SQL type name (e.g., "INT", "NUMERIC")
   * @param columnType      JDBC type constant
   * @param precision       Numeric precision
   * @param scale           Numeric scale
   * @param columnName      Column name (for logging)
   * @param isSigned        Whether the column is signed
   * @param handleAsDecimal Whether to treat as decimal
   * @return Corresponding CDAP {@link Schema}
   */
  @Override
  public Schema getSchema(String typeName, int columnType, int precision, int scale, String columnName,
                          boolean isSigned, boolean handleAsDecimal) {
    if (STRING_MAPPED_POSTGRES_TYPES_NAMES.contains(typeName) || STRING_MAPPED_POSTGRES_TYPES.contains(columnType)) {
      return Schema.of(Schema.Type.STRING);
    }
    // If it is a numeric type without precision then use the Schema of String to avoid any precision loss
    if (Types.NUMERIC == columnType) {
      if (precision == 0) {
        LOG.warn(String.format("Field '%s' is a %s type without precision and scale, "
                        + "converting into STRING type to avoid any precision loss.",
                columnName, typeName));
        return Schema.of(Schema.Type.STRING);
      }
      return Schema.decimalOf(precision, scale);
    }

    if ("timestamp".equalsIgnoreCase(typeName)) {
      return Schema.of(Schema.LogicalType.DATETIME);
    }
    return super.getSchema(typeName, columnType, precision, scale, columnName, isSigned, handleAsDecimal);
  }
}

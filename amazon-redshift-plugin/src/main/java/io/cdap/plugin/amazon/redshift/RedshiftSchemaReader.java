/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.amazon.redshift;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Set;

/**
 * Redshift Schema Reader class
 */
public class RedshiftSchemaReader extends CommonSchemaReader {

  private static final Logger LOG = LoggerFactory.getLogger(RedshiftSchemaReader.class);

  public static final Set<String> STRING_MAPPED_REDSHIFT_TYPES_NAMES = ImmutableSet.of(
    "timetz", "money"
  );

  private final String sessionID;

  public RedshiftSchemaReader() {
    this(null);
  }

  public RedshiftSchemaReader(String sessionID) {
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

  @Override
  public List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      if (shouldIgnoreColumn(metadata, i)) {
        continue;
      }
      String columnName = metadata.getColumnName(i);
      Schema columnSchema = getSchema(metadata, i);
      // Setting up schema as nullable as cdata driver doesn't provide proper information about isNullable.
      columnSchema = Schema.nullableOf(columnSchema);
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  /**
   * Returns the CDAP {@link Schema} for a database column based on JDBC metadata.
   * Handles Redshift-specific and common JDBC types:
   * Maps Redshift string types to {@link Schema.Type#STRING}
   * Maps "INT" to {@link Schema.Type#INT}
   * Maps "BIGINT" to {@link Schema.Type#LONG}.
   * Maps NUMERIC with zero precision to {@link Schema.Type#STRING} and logs a warning.
   * Maps "timestamp" to {@link Schema.LogicalType#DATETIME}.
   * Delegates to the parent plugin for all other types.
   * @param typeName    SQL type name (e.g. "INT", "BIGINT", "timestamp")
   * @param columnType  JDBC type code (see {@link java.sql.Types})
   * @param precision   column precision (for numeric types)
   * @param scale       column scale (for numeric types)
   * @param columnName  column name
   * @param isSigned    whether the column is signed
   * @param handleAsDecimal whether to handle as decimal
   * @return the mapped {@link Schema} type
   */
  @Override
  public Schema getSchema(String typeName, int columnType, int precision, int scale, String columnName,
                          boolean isSigned, boolean handleAsDecimal) {
    if (STRING_MAPPED_REDSHIFT_TYPES_NAMES.contains(typeName)) {
      return Schema.of(Schema.Type.STRING);
    }
    if ("INT".equalsIgnoreCase(typeName)) {
      return Schema.of(Schema.Type.INT);
    }
    if ("BIGINT".equalsIgnoreCase(typeName)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (Types.NUMERIC == columnType && precision == 0) {
      LOG.warn(String.format("Field '%s' is a %s type without precision and scale," +
                      " converting into STRING type to avoid any precision loss.",
              columnName, typeName));
      return Schema.of(Schema.Type.STRING);
    }
    if ("timestamp".equalsIgnoreCase(typeName)) {
      return Schema.of(Schema.LogicalType.DATETIME);
    }
    return super.getSchema(typeName, columnType, precision, scale, columnName, isSigned, handleAsDecimal);
  }
}

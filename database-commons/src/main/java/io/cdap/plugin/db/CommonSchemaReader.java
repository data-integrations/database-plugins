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

package io.cdap.plugin.db;

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.db.DBUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Common schema reader for mapping non specific DB types.
 */
public class CommonSchemaReader implements SchemaReader {
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
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    return DBUtils.getSchema(metadata.getColumnTypeName(index), metadata.getColumnType(index),
                             metadata.getPrecision(index), metadata.getScale(index), metadata.getColumnName(index),
                             metadata.isSigned(index), true);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    return false;
  }

  /**
   * Returns the schema fields for the specified table using JDBC metadata.
   * Supports schema-qualified table names (e.g. "schema.table").
   * Throws SQLException if the table has no columns.
   *
   * @param connection JDBC connection
   * @param tableName table name, optionally schema-qualified
   * @return list of schema fields
   * @throws SQLException if no columns found or on database error
   */
  @Override
    public List<Schema.Field> getSchemaFields(Connection connection, String tableName) throws SQLException {
    DatabaseMetaData dbMetaData = connection.getMetaData();
    String schema = null;
    String table = tableName;
    // Support schema-qualified table names like "schema.table"
    if (tableName != null && tableName.contains(".")) {
      String[] parts = tableName.split("\\.", 2);
      schema = parts[0];
      table = parts[1];
    }
    try (ResultSet columns = dbMetaData.getColumns(null, schema, table, null)) {
      List<Schema.Field> schemaFields = Lists.newArrayList();
      while (columns.next()) {
        String columnName = columns.getString("COLUMN_NAME");
        String typeName = columns.getString("TYPE_NAME");
        int columnType = columns.getInt("DATA_TYPE");
        int precision = columns.getInt("COLUMN_SIZE");
        int scale = columns.getInt("DECIMAL_DIGITS");
        int nullable = columns.getInt("NULLABLE");

        Schema columnSchema = this.getSchema(typeName, columnType, precision, scale, columnName, true, true);
        if (nullable == DatabaseMetaData.columnNullable) {
          columnSchema = Schema.nullableOf(columnSchema);
        }
        Schema.Field field = Schema.Field.of(columnName, columnSchema);
        schemaFields.add(field);
      }
      if (schemaFields.isEmpty()) {
        throw new SQLException("No columns found for table: " +
                (schema != null ? schema + "." : "") + table);
      }
      return schemaFields;
    }
  }

  /**
   * Returns the CDAP schema for the given SQL column type.
   *
   * @param typeName SQL type name
   * @param columnType JDBC type code
   * @param precision Numeric precision
   * @param scale Numeric scale
   * @param columnName Column name
   * @param isSigned Whether the column is signed
   * @param handleAsDecimal Whether to treat as decimal
   * @return Corresponding {@link Schema}, or null if not implemented
   */
  public Schema getSchema(String typeName, int columnType, int precision, int scale, String columnName ,
                          boolean isSigned, boolean handleAsDecimal) {
    return DBUtils.getSchema(typeName, columnType, precision, scale, columnName, isSigned, handleAsDecimal);
  }
}

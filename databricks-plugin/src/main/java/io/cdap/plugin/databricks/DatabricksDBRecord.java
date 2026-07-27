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

package io.cdap.plugin.databricks;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Writable class for Databricks Source
 */
public class DatabricksDBRecord extends DBRecord {

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public DatabricksDBRecord() {
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new DatabricksSchemaReader();
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    String columnTypeName = metadata.getColumnTypeName(columnIndex);
    String normalizedType = columnTypeName != null ? columnTypeName.trim().toUpperCase() : null;

    if (sqlType == Types.NULL || "VOID".equals(normalizedType) || "NULL".equals(normalizedType)) {
      recordBuilder.set(field.getName(), null);
      return;
    }

    if (normalizedType != null && (normalizedType.equals("VARIANT") ||
        normalizedType.startsWith("ARRAY") || normalizedType.startsWith("MAP") ||
        normalizedType.startsWith("STRUCT") || normalizedType.equals("OBJECT") ||
        normalizedType.equals("FILE") || normalizedType.startsWith("INTERVAL") ||
        normalizedType.startsWith("GEOGRAPHY") || normalizedType.startsWith("GEOMETRY"))) {
      Object value = resultSet.getObject(columnIndex);
      recordBuilder.set(field.getName(), value != null ? value.toString() : null);
      return;
    }

    Schema nonNullableSchema = field.getSchema().isNullable() ?
      field.getSchema().getNonNullable() : field.getSchema();
    if (Schema.LogicalType.DATETIME.equals(nonNullableSchema.getLogicalType()) ||
        "TIMESTAMP_NTZ".equals(normalizedType)) {
      Timestamp timestamp = resultSet.getTimestamp(columnIndex);
      recordBuilder.setDateTime(field.getName(), timestamp != null ? timestamp.toLocalDateTime() : null);
      return;
    }

    setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
  }
}

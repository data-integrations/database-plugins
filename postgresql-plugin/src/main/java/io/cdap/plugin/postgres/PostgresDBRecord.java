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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.Operation;
import io.cdap.plugin.db.SchemaReader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Writable class for PostgreSQL Source/Sink
 */
public class PostgresDBRecord extends DBRecord {

  public PostgresDBRecord(StructuredRecord record, List<ColumnType> columnTypes, Operation operationName,
                          String relationTableKey) {
    super(record, columnTypes, operationName, relationTableKey);
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public PostgresDBRecord() {
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    if (isUseSchema(metadata, columnIndex)) {
      setFieldAccordingToSchema(resultSet, recordBuilder, field, columnIndex);
    } else {
      int columnType = metadata.getColumnType(columnIndex);
      if (columnType == Types.NUMERIC) {
        Schema nonNullableSchema = field.getSchema().isNullable() ?
                field.getSchema().getNonNullable() : field.getSchema();
        int precision = metadata.getPrecision(columnIndex);
        if (precision == 0 && Schema.Type.STRING.equals(nonNullableSchema.getType())) {
          // When output schema is set to String for precision less numbers
          recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        } else if (Schema.LogicalType.DECIMAL.equals(nonNullableSchema.getLogicalType())) {
          BigDecimal orgValue = resultSet.getBigDecimal(columnIndex);
          if (orgValue != null) {
            BigDecimal decimalValue = new BigDecimal(orgValue.toPlainString())
                    .setScale(nonNullableSchema.getScale(), RoundingMode.HALF_EVEN);
            recordBuilder.setDecimal(field.getName(), decimalValue);
          }
        }
        return;
      }
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  private static boolean isUseSchema(ResultSetMetaData metadata, int columnIndex) throws SQLException {
    String columnTypeName = metadata.getColumnTypeName(columnIndex);
    // If the column Type Name is present in the String mapped PostgreSQL types then return true.
    if (PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES_NAMES.contains(columnTypeName)
        || PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES.contains(metadata.getColumnType(columnIndex))) {
      return true;
    }

    return false;
  }

  private Object createPGobject(String type, String value, ClassLoader classLoader) throws SQLException {
    try {
      Class pGObjectClass = classLoader.loadClass("org.postgresql.util.PGobject");
      Method setTypeMethod = pGObjectClass.getMethod("setType", String.class);
      Method setValueMethod = pGObjectClass.getMethod("setValue", String.class);
      Object result = pGObjectClass.newInstance();
      setTypeMethod.invoke(result, type);
      setValueMethod.invoke(result, value);
      return result;
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
      InvocationTargetException e) {
      throw new SQLException("Failed to create instance of org.postgresql.util.PGobject");
    }
  }

  @Override
  protected void writeNonNullToDB(PreparedStatement stmt, Schema fieldSchema,
                                  String fieldName, int fieldIndex) throws SQLException {
    int sqlIndex = fieldIndex + 1;
    ColumnType columnType = modifiableColumnTypes.get(fieldIndex);
    if (PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES_NAMES.contains(columnType.getTypeName()) ||
      PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES.contains(columnType.getType())) {
      stmt.setObject(sqlIndex, createPGobject(columnType.getTypeName(),
                                              record.get(fieldName),
                                              stmt.getClass().getClassLoader()));
      return;
    } else if (columnType.getType() == Types.NUMERIC) {
      if (record.get(fieldName) != null) {
        if (fieldSchema.getType() == Schema.Type.STRING) {
          stmt.setBigDecimal(sqlIndex, new BigDecimal((String) record.get(fieldName)));
          return;
        }
      }
    }

    super.writeNonNullToDB(stmt, fieldSchema, fieldName, fieldIndex);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new PostgresSchemaReader();
  }

  @Override
  protected void upsertOperation(PreparedStatement stmt) throws SQLException {
    for (int fieldIndex = 0; fieldIndex < columnTypes.size(); fieldIndex++) {
      ColumnType columnType = columnTypes.get(fieldIndex);
      Schema.Field field = record.getSchema().getField(columnType.getName());
      writeToDB(stmt, field, fieldIndex);
    }
  }
}

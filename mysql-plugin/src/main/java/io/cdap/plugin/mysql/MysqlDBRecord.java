/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.mysql;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Writable class for MySQL Source/Sink.
 */
public class MysqlDBRecord extends DBRecord {

  public MysqlDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public MysqlDBRecord() {
    // Required by Hadoop DBRecordReader to create an instance
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    // Convert MySQL YEAR type to integer if the output schema is set to CDAP int type.
    // The deprecated conversion of the YEAR type to Date is still supported in the super.handleField call.
    if (sqlType == Types.DATE
        && MysqlSchemaReader.YEAR_TYPE_NAME.equalsIgnoreCase(resultSet.getMetaData().getColumnTypeName(columnIndex))) {
      Schema schema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      if (Schema.Type.INT.equals(schema.getType())
          && !Schema.LogicalType.DATE.equals(schema.getLogicalType())) {
        Date date = resultSet.getDate(columnIndex);
        recordBuilder.set(field.getName(), date != null ? resultSet.getInt(columnIndex) : null);
        return;
      }
    }

    // Deprecated : Handle the Tinyint(1) to boolean conversion use case.
    if (sqlType == Types.TINYINT && resultSet.getObject(columnIndex) != null) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
              field.getSchema().getNonNullable() : field.getSchema();
      switch (nonNullableSchema.getType()) {
        case BOOLEAN:
          recordBuilder.set(field.getName(), resultSet.getBoolean(columnIndex));
          return;
      }
    }

    super.handleField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
  }

  @Override
  protected void writeNonNullToDB(PreparedStatement stmt, Schema fieldSchema,
                                  String fieldName, int fieldIndex) throws SQLException {

    int sqlType = columnTypes.get(fieldIndex).getType();
    int sqlIndex = fieldIndex + 1;
    switch (sqlType) {
      case Types.DATE:
        if (fieldSchema.getType().equals(Schema.Type.INT)
                && !Schema.LogicalType.DATE.equals(fieldSchema.getLogicalType())) {
          Object fieldValue = record.get(fieldName);
          stmt.setInt(sqlIndex, (Integer) fieldValue);
          return;
        }
    }

    super.writeNonNullToDB(stmt, fieldSchema, fieldName, fieldIndex);
  }

  @Override
  protected void insertOperation(PreparedStatement stmt) throws SQLException {
    for (int fieldIndex = 0; fieldIndex < columnTypes.size(); fieldIndex++) {
      ColumnType columnType = columnTypes.get(fieldIndex);
      Schema.Field field = record.getSchema().getField(columnType.getName(), true);
      writeToDB(stmt, field, fieldIndex);
    }
  }
}

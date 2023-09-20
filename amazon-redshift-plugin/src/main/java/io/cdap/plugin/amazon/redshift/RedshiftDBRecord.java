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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.util.DBUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Writable class for Redshift Source
 */
public class RedshiftDBRecord extends DBRecord {

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public RedshiftDBRecord() {
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    String columnTypeName = metadata.getColumnTypeName(columnIndex);
    if (isUseSchema(metadata, columnIndex)) {
      setFieldAccordingToSchema(resultSet, recordBuilder, field, columnIndex);
      return;
    }

    // HandleTimestamp
    if (sqlType == Types.TIMESTAMP && columnTypeName.equalsIgnoreCase("timestamp")) {
      Timestamp timestamp = resultSet.getTimestamp(columnIndex, DBUtils.PURE_GREGORIAN_CALENDAR);
      if (timestamp != null) {
        ZonedDateTime zonedDateTime = OffsetDateTime.of(timestamp.toLocalDateTime(), OffsetDateTime.now().getOffset())
          .atZoneSameInstant(ZoneId.of("UTC"));
        Schema nonNullableSchema = field.getSchema().isNullable() ?
          field.getSchema().getNonNullable() : field.getSchema();
        setZonedDateTimeBasedOnOutputSchema(recordBuilder, nonNullableSchema.getLogicalType(),
                                           field.getName(), zonedDateTime);
      } else {
        recordBuilder.set(field.getName(), null);
      }
      return;
    }

    // HandleTimestampTZ
    if (sqlType == Types.TIMESTAMP && columnTypeName.equalsIgnoreCase("timestamptz")) {
      OffsetDateTime timestamp = resultSet.getObject(columnIndex, OffsetDateTime.class);
      if (timestamp != null) {
        recordBuilder.setTimestamp(field.getName(), timestamp.atZoneSameInstant(ZoneId.of("UTC")));
      } else {
        recordBuilder.set(field.getName(), null);
      }
      return;
    }

    // HandleNumeric
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

  private void setZonedDateTimeBasedOnOutputSchema(StructuredRecord.Builder recordBuilder,
                                                  Schema.LogicalType logicalType,
                                                  String fieldName,
                                                  ZonedDateTime zonedDateTime) {
    if (Schema.LogicalType.DATETIME.equals(logicalType)) {
      recordBuilder.setDateTime(fieldName, zonedDateTime.toLocalDateTime());
    } else if (Schema.LogicalType.TIMESTAMP_MICROS.equals(logicalType)) {
      recordBuilder.setTimestamp(fieldName, zonedDateTime);
    }
  }

  private static boolean isUseSchema(ResultSetMetaData metadata, int columnIndex) throws SQLException {
    String columnTypeName = metadata.getColumnTypeName(columnIndex);
    // If the column Type Name is present in the String mapped Redshift types then return true.
    return RedshiftSchemaReader.STRING_MAPPED_REDSHIFT_TYPES_NAMES.contains(columnTypeName)
      || RedshiftSchemaReader.STRING_MAPPED_REDSHIFT_TYPES.contains(metadata.getColumnType(columnIndex));
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new RedshiftSchemaReader();
  }

}

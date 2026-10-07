/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Utility class to convert and map Oracle STRUCT attribute values into CDAP {@link StructuredRecord} fields.
 */
public class OracleStructUtil {

    private OracleStructUtil() {
    }

    /**
     * Converts a raw Oracle STRUCT attribute value and populates it into the record builder.
     * Falls back to standard DBRecord field population for non-Oracle specific types.
     */
    public static void populateRecordField(OracleSourceDBRecord sourceRecord, Connection connection,
                                           StructuredRecord.Builder recordBuilder, Schema.Field field,
                                           Object attrValue) throws SQLException {
        if (attrValue == null) {
            recordBuilder.set(field.getName(), null);
            return;
        }

        Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
                : field.getSchema();
        String attrClassName = attrValue.getClass().getName();
        if (attrValue instanceof Struct) {
            recordBuilder.set(field.getName(), sourceRecord.convertStructToRecord((Struct) attrValue,
                    fieldSchema, connection));
            return;
        }
        if (attrValue instanceof Clob) {
            Clob clob = (Clob) attrValue;
            recordBuilder.set(field.getName(), clob.getSubString(1, (int) clob.length()));
            return;
        }
        if (attrValue instanceof Blob) {
            Blob blob = (Blob) attrValue;
            recordBuilder.set(field.getName(), blob.getBytes(1, (int) blob.length()));
            return;
        }
        if (attrValue instanceof SQLXML) {
            recordBuilder.set(field.getName(), ((SQLXML) attrValue).getString());
            return;
        }
        if ("oracle.sql.INTERVALDS".equals(attrClassName) || "oracle.sql.INTERVALYM".equals(attrClassName)) {
            recordBuilder.set(field.getName(), attrValue.toString());
            return;
        }
        if (attrValue instanceof BigDecimal) {
            handleDecimalValue((BigDecimal) attrValue, fieldSchema, recordBuilder, field);
            return;
        }
        if (attrValue instanceof Timestamp) {
            handleTimestampValue(sourceRecord, (Timestamp) attrValue, fieldSchema, recordBuilder,
                    field, connection);
            return;
        }
        if (attrValue instanceof OffsetDateTime) {
            handleOffsetDateTimeValue((OffsetDateTime) attrValue, fieldSchema, recordBuilder, field);
            return;
        }
        if (isBfileValue(attrValue, field.getName())) {
            recordBuilder.set(field.getName(), sourceRecord.getBfileBytes(attrValue, field.getName()));
            return;
        }

        sourceRecord.populateRecordField(connection, recordBuilder, field, attrValue);
    }

    private static void handleTimestampValue(OracleSourceDBRecord sourceRecord, Timestamp timestamp, Schema fieldSchema,
                                      StructuredRecord.Builder recordBuilder, Schema.Field field,
                                      Connection connection) throws SQLException {
        if (Schema.LogicalType.DATETIME.equals(fieldSchema.getLogicalType())) {
            recordBuilder.setDateTime(field.getName(), timestamp.toLocalDateTime());
        } else if (Schema.LogicalType.DATE.equals(fieldSchema.getLogicalType())) {
            recordBuilder.setDate(field.getName(), timestamp.toLocalDateTime().toLocalDate());
        } else if (fieldSchema.getType() == Schema.Type.STRING) {
            recordBuilder.set(field.getName(), timestamp.toString());
        } else {
            sourceRecord.populateRecordField(connection, recordBuilder, field, timestamp);
        }
    }

    private static void handleOffsetDateTimeValue(OffsetDateTime offsetDateTime, Schema fieldSchema,
                                           StructuredRecord.Builder recordBuilder, Schema.Field field) {
        ZonedDateTime zonedDateTime = offsetDateTime.atZoneSameInstant(ZoneId.of("UTC"));
        if (fieldSchema.getLogicalType() != null &&
                (Schema.LogicalType.TIMESTAMP_MICROS.equals(fieldSchema.getLogicalType()) ||
                        Schema.LogicalType.TIMESTAMP_MILLIS.equals(fieldSchema.getLogicalType()))) {
            recordBuilder.setTimestamp(field.getName(), zonedDateTime);
        } else if (Schema.LogicalType.DATETIME.equals(fieldSchema.getLogicalType())) {
            recordBuilder.setDateTime(field.getName(), offsetDateTime.toLocalDateTime());
        } else {
            recordBuilder.set(field.getName(), zonedDateTime.toString());
        }
    }

    /**
     * Checks if the given attribute object is an instance of Oracle BFILE via reflection.
     */
    private static boolean isBfileValue(Object attrValue, String fieldName) throws SQLException {
        ClassLoader oracleLoader = attrValue.getClass().getClassLoader();
        try {
            if (oracleLoader != null && oracleLoader.loadClass("oracle.jdbc.OracleBfile").isInstance(attrValue)) {
                return true;
            }
        } catch (ClassNotFoundException e) {
            throw new SQLException(String.format("Column '%s' is of type 'BFILE', which is not supported with " +
                    "this version of the JDBC driver.", fieldName), e);
        }
        return false;
    }

    private static void handleDecimalValue(BigDecimal bigDecimal, Schema fieldSchema,
                                    StructuredRecord.Builder recordBuilder, Schema.Field field) {
        if (Schema.LogicalType.DECIMAL.equals(fieldSchema.getLogicalType())) {
            recordBuilder.setDecimal(field.getName(), bigDecimal.setScale(fieldSchema.getScale(),
                    RoundingMode.HALF_UP));
            return;
        }
        switch (fieldSchema.getType()) {
            case DOUBLE:
                recordBuilder.set(field.getName(), bigDecimal.doubleValue());
                break;
            case FLOAT:
                recordBuilder.set(field.getName(), bigDecimal.floatValue());
                break;
            case INT:
                recordBuilder.set(field.getName(), bigDecimal.intValue());
                break;
            case LONG:
                recordBuilder.set(field.getName(), bigDecimal.longValue());
                break;
            case STRING:
                recordBuilder.set(field.getName(), bigDecimal.toPlainString());
                break;
            default:
                recordBuilder.set(field.getName(), bigDecimal);
        }
    }
}

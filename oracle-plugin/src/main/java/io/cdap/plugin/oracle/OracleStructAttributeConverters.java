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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Standalone registry of AttributeConverter strategies for converting Oracle STRUCT attributes.
 */
public final class OracleStructAttributeConverters {

  /**
   * Strategy interface for translating structured attributes to CDAP records.
   */
  public interface AttributeConverter {
    boolean canConvert(Object attrValue, String attrClassName);
    void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                 Object attrValue) throws SQLException;
  }

  private static class FloatConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof Float;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      Float floatVal = (Float) attrValue;
      builder.set(field.getName(), floatVal);
    }
  }

  private static class DoubleConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof Double;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      Double doubleVal = (Double) attrValue;
      builder.set(field.getName(), doubleVal);
    }
  }

  private static class BigDecimalConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof BigDecimal;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      BigDecimal bigDecimal = (BigDecimal) attrValue;
      if (Schema.LogicalType.DECIMAL.equals(fieldSchema.getLogicalType())) {
        builder.setDecimal(field.getName(), bigDecimal.setScale(fieldSchema.getScale(),
                java.math.RoundingMode.HALF_UP));
      } else if (Schema.Type.DOUBLE.equals(fieldSchema.getType())) {
        builder.set(field.getName(), bigDecimal.doubleValue());
      } else if (Schema.Type.FLOAT.equals(fieldSchema.getType())) {
        builder.set(field.getName(), bigDecimal.floatValue());
      } else if (Schema.Type.INT.equals(fieldSchema.getType())) {
        builder.set(field.getName(), bigDecimal.intValue());
      } else {
        builder.set(field.getName(), bigDecimal.toString());
      }
    }
  }

  private static class TimestampConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof Timestamp;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      Timestamp timestamp = (Timestamp) attrValue;
      if (Schema.LogicalType.DATETIME.equals(fieldSchema.getLogicalType())) {
        builder.setDateTime(field.getName(), timestamp.toLocalDateTime());
      } else if (Schema.LogicalType.DATE.equals(fieldSchema.getLogicalType())) {
        builder.setDate(field.getName(), timestamp.toLocalDateTime().toLocalDate());
      } else {
        builder.set(field.getName(), attrValue.toString());
      }
    }
  }

  private static class ZonedDateTimeConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof OffsetDateTime || attrValue instanceof ZonedDateTime;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      ZonedDateTime zonedDateTime = (attrValue instanceof OffsetDateTime)
          ? ((OffsetDateTime) attrValue).atZoneSameInstant(ZoneId.of("UTC"))
          : ((ZonedDateTime) attrValue).withZoneSameInstant(ZoneId.of("UTC"));
      if (fieldSchema.getLogicalType() != null &&
          (Schema.LogicalType.TIMESTAMP_MICROS.equals(fieldSchema.getLogicalType()) ||
              Schema.LogicalType.TIMESTAMP_MILLIS.equals(fieldSchema.getLogicalType()))) {
        builder.setTimestamp(field.getName(), zonedDateTime);
      } else if (Schema.Type.LONG.equals(fieldSchema.getType())) {
        builder.set(field.getName(), zonedDateTime.toInstant().toEpochMilli());
      } else {
        builder.set(field.getName(), zonedDateTime.toString());
      }
    }
  }

  private static class ClobConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof Clob;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      Clob clob = (Clob) attrValue;
      builder.set(field.getName(), clob.getSubString(1, (int) clob.length()));
    }
  }

  private static class BlobConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof Blob;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      Blob blob = (Blob) attrValue;
      builder.set(field.getName(), blob.getBytes(1, (int) blob.length()));
    }
  }

  private static class OracleBfileConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      try {
        ClassLoader oracleLoader = attrValue.getClass().getClassLoader();
        Class<?> bfileInterface = oracleLoader.loadClass("oracle.jdbc.OracleBfile");
        return bfileInterface.isInstance(attrValue);
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      builder.set(field.getName(), OracleSourceDBRecord.getBfileBytes(attrValue, field.getName()));
    }
  }

  private static class ByteArrayConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof byte[];
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      builder.set(field.getName(), (byte[]) attrValue);
    }
  }

  private static class OracleIntervalConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return "oracle.sql.INTERVALDS".equals(attrClassName) || "oracle.sql.INTERVALYM".equals(attrClassName);
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      builder.set(field.getName(), attrValue.toString());
    }
  }

  private static class SqlXmlConverter implements AttributeConverter {

    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return attrValue instanceof SQLXML;
    }

    @Override
    public void convert(
            StructuredRecord.Builder builder,
            Schema.Field field,
            Schema fieldSchema,
            Object attrValue) throws SQLException {

      SQLXML xml = (SQLXML) attrValue;
      builder.set(field.getName(), xml.getString());
    }
  }

  private static class DefaultConverter implements AttributeConverter {
    @Override
    public boolean canConvert(Object attrValue, String attrClassName) {
      return true;
    }

    @Override
    public void convert(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                        Object attrValue) throws SQLException {
      builder.set(field.getName(), attrValue);
    }
  }

  private static final List<AttributeConverter> CONVERTERS = Arrays.asList(
      new BigDecimalConverter(),
      new TimestampConverter(),
      new ZonedDateTimeConverter(),
      new ClobConverter(),
      new BlobConverter(),
      new OracleBfileConverter(),
      new ByteArrayConverter(),
      new OracleIntervalConverter(),
      new SqlXmlConverter(),
      new FloatConverter(),
      new DoubleConverter(),
      new DefaultConverter()
  );

  private OracleStructAttributeConverters() {
    // Private constructor to prevent instantiation.
  }

  /**
   * Translates an Oracle STRUCT attribute to a CDAP structured record field.
   */
  public static void convertValue(StructuredRecord.Builder builder, Schema.Field field, Schema fieldSchema,
                                  Object attrValue, String attrClassName) throws SQLException {
    for (AttributeConverter converter : CONVERTERS) {
      if (converter.canConvert(attrValue, attrClassName)) {
        converter.convert(builder, field, fieldSchema, attrValue);
        break;
      }
    }
  }
}

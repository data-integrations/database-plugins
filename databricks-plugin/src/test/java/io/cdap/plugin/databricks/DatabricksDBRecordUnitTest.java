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
import io.cdap.plugin.util.DBUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Unit tests for {@link DatabricksDBRecord}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DatabricksDBRecordUnitTest {

  @Test
  public void testHandleFieldTimestampAndTimestampNtz() throws SQLException {
    OffsetDateTime offsetDateTime = OffsetDateTime.of(2026, 4, 16, 10, 30, 0, 123456000, ZoneOffset.UTC);
    LocalDateTime localDateTime = LocalDateTime.of(2026, 4, 16, 10, 30, 0, 123456000);

    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metaData.getColumnTypeName(1)).thenReturn("TIMESTAMP");
    Mockito.when(metaData.getColumnTypeName(2)).thenReturn("TIMESTAMP_NTZ");

    Timestamp ts = Timestamp.from(offsetDateTime.toInstant());
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(resultSet.getObject(1)).thenReturn(ts);
    Mockito.when(resultSet.getTimestamp(1, DBUtils.PURE_GREGORIAN_CALENDAR)).thenReturn(ts);
    Mockito.when(resultSet.getTimestamp(2)).thenReturn(Timestamp.valueOf(localDateTime));

    Schema.Field tsField = Schema.Field.of("ts_col", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    Schema.Field tsNtzField = Schema.Field.of("ts_ntz_col", Schema.of(Schema.LogicalType.DATETIME));
    Schema schema = Schema.recordOf("dbRecord", tsField, tsNtzField);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    DatabricksDBRecord dbRecord = new DatabricksDBRecord();
    dbRecord.handleField(resultSet, builder, tsField, 1, Types.TIMESTAMP, 0, 0);
    dbRecord.handleField(resultSet, builder, tsNtzField, 2, Types.TIMESTAMP, 0, 0);

    StructuredRecord record = builder.build();
    Assert.assertEquals(offsetDateTime.toInstant(), record.getTimestamp("ts_col", ZoneId.of("UTC")).toInstant());
    Assert.assertEquals(localDateTime, record.getDateTime("ts_ntz_col"));
  }

  @Test
  public void testHandleFieldDateAndDecimal() throws SQLException {
    LocalDate expectedDate = LocalDate.of(2026, 4, 16);
    Date sqlDate = Date.valueOf(expectedDate);
    BigDecimal expectedDecimal = new BigDecimal("12345678901234567890.1234567890");

    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metaData.getColumnTypeName(1)).thenReturn("DATE");
    Mockito.when(metaData.getColumnTypeName(2)).thenReturn("DECIMAL");

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(resultSet.getObject(1)).thenReturn(sqlDate);
    Mockito.when(resultSet.getDate(1)).thenReturn(sqlDate);
    Mockito.when(resultSet.getObject(2)).thenReturn(expectedDecimal);

    Schema.Field dateField = Schema.Field.of("date_col", Schema.of(Schema.LogicalType.DATE));
    Schema.Field decimalField = Schema.Field.of("dec_col", Schema.decimalOf(38, 10));
    Schema schema = Schema.recordOf("dbRecord", dateField, decimalField);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    DatabricksDBRecord dbRecord = new DatabricksDBRecord();
    dbRecord.handleField(resultSet, builder, dateField, 1, Types.DATE, 0, 0);
    dbRecord.handleField(resultSet, builder, decimalField, 2, Types.DECIMAL, 38, 10);

    StructuredRecord record = builder.build();
    Assert.assertEquals(expectedDate, record.getDate("date_col"));
    Assert.assertEquals(expectedDecimal, record.getDecimal("dec_col"));
  }

  @Test
  public void testHandleFieldComplexAndNullTypes() throws SQLException {
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metaData.getColumnTypeName(1)).thenReturn("ARRAY");
    Mockito.when(metaData.getColumnTypeName(2)).thenReturn("MAP");
    Mockito.when(metaData.getColumnTypeName(3)).thenReturn("STRUCT");
    Mockito.when(metaData.getColumnTypeName(4)).thenReturn("VARIANT");
    Mockito.when(metaData.getColumnTypeName(5)).thenReturn("VOID");

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(resultSet.getObject(1)).thenReturn("[10001,10002]");
    Mockito.when(resultSet.getObject(2)).thenReturn("{\"pickup\":10001}");
    Mockito.when(resultSet.getObject(3)).thenReturn("{\"distance\":2.5,\"fare\":15.5}");
    Mockito.when(resultSet.getObject(4)).thenReturn("{\"vendor\":1}");

    Schema.Field arrayField = Schema.Field.of("array_col", Schema.of(Schema.Type.STRING));
    Schema.Field mapField = Schema.Field.of("map_col", Schema.of(Schema.Type.STRING));
    Schema.Field structField = Schema.Field.of("struct_col", Schema.of(Schema.Type.STRING));
    Schema.Field variantField = Schema.Field.of("variant_col", Schema.of(Schema.Type.STRING));
    Schema.Field voidField = Schema.Field.of("void_col", Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("dbRecord", arrayField, mapField, structField, variantField, voidField);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    DatabricksDBRecord dbRecord = new DatabricksDBRecord();
    dbRecord.handleField(resultSet, builder, arrayField, 1, Types.ARRAY, 0, 0);
    dbRecord.handleField(resultSet, builder, mapField, 2, Types.OTHER, 0, 0);
    dbRecord.handleField(resultSet, builder, structField, 3, Types.STRUCT, 0, 0);
    dbRecord.handleField(resultSet, builder, variantField, 4, Types.OTHER, 0, 0);
    dbRecord.handleField(resultSet, builder, voidField, 5, Types.NULL, 0, 0);

    StructuredRecord record = builder.build();
    Assert.assertEquals("[10001,10002]", record.get("array_col"));
    Assert.assertEquals("{\"pickup\":10001}", record.get("map_col"));
    Assert.assertEquals("{\"distance\":2.5,\"fare\":15.5}", record.get("struct_col"));
    Assert.assertEquals("{\"vendor\":1}", record.get("variant_col"));
    Assert.assertNull(record.get("void_col"));
  }
}

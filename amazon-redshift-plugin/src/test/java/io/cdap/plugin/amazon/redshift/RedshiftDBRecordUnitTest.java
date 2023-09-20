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
import io.cdap.plugin.util.DBUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit Test class for the PostgresDBRecord
 */
@RunWith(MockitoJUnitRunner.class)
public class RedshiftDBRecordUnitTest {

  private static final int DEFAULT_PRECISION = 38;

  /**
   * Validate the precision less Numbers handling against following use cases.
   * 1. Ensure that the numeric type with [p,s] set as [38,4] detect as BigDecimal(38,4) in cdap.
   * 2. Ensure that the numeric type without [p,s] detect as String type in cdap.
   *
   * @throws Exception
   */
  @Test
  public void validatePrecisionLessDecimalParsing() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.decimalOf(DEFAULT_PRECISION, 4));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.of(Schema.Type.STRING));

    Schema schema = Schema.recordOf(
      "dbRecord",
      field1,
      field2
    );

    ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(resultSetMetaData.getColumnType(Mockito.eq(1))).thenReturn(Types.NUMERIC);
    Mockito.when(resultSetMetaData.getPrecision(Mockito.eq(1))).thenReturn(DEFAULT_PRECISION);
    Mockito.when(resultSetMetaData.getColumnType(eq(2))).thenReturn(Types.NUMERIC);
    when(resultSetMetaData.getPrecision(eq(2))).thenReturn(0);

    ResultSet resultSet = Mockito.mock(ResultSet.class);

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getBigDecimal(eq(1))).thenReturn(BigDecimal.valueOf(123.4568));
    when(resultSet.getString(eq(2))).thenReturn("123.4568");

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    RedshiftDBRecord dbRecord = new RedshiftDBRecord();
    dbRecord.handleField(resultSet, builder, field1, 1, Types.NUMERIC, DEFAULT_PRECISION, 4);
    dbRecord.handleField(resultSet, builder, field2, 2, Types.NUMERIC, 0, -127);

    StructuredRecord record = builder.build();
    Assert.assertTrue(record.getDecimal("ID1") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID1"), BigDecimal.valueOf(123.4568));
    Assert.assertTrue(record.get("ID2") instanceof String);
    Assert.assertEquals(record.get("ID2"), "123.4568");
  }

  @Test
  public void validateTimestampType() throws SQLException {
    OffsetDateTime offsetDateTime = OffsetDateTime.of(2023, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
    when(metaData.getColumnTypeName(eq(0))).thenReturn("timestamp");

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(resultSet.getTimestamp(eq(0), eq(DBUtils.PURE_GREGORIAN_CALENDAR)))
      .thenReturn(Timestamp.from(offsetDateTime.toInstant()));

    Schema.Field field1 = Schema.Field.of("field1", Schema.of(Schema.LogicalType.DATETIME));
    Schema schema = Schema.recordOf(
      "dbRecord",
      field1
    );
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    RedshiftDBRecord dbRecord = new RedshiftDBRecord();
    dbRecord.handleField(resultSet, builder, field1, 0, Types.TIMESTAMP, 0, 0);
    StructuredRecord record = builder.build();
    Assert.assertNotNull(record);
    Assert.assertNotNull(record.getDateTime("field1"));
    Assert.assertEquals(record.getDateTime("field1").toInstant(ZoneOffset.UTC), offsetDateTime.toInstant());

    // Validate backward compatibility

    field1 = Schema.Field.of("field1", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    schema = Schema.recordOf(
      "dbRecord",
      field1
    );
    builder = StructuredRecord.builder(schema);
    dbRecord.handleField(resultSet, builder, field1, 0, Types.TIMESTAMP, 0, 0);
    record = builder.build();
    Assert.assertNotNull(record);
    Assert.assertNotNull(record.getTimestamp("field1"));
    Assert.assertEquals(record.getTimestamp("field1").toInstant(), offsetDateTime.toInstant());
  }

  @Test
  public void validateTimestampTZType() throws SQLException {
    OffsetDateTime offsetDateTime = OffsetDateTime.of(2023, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
    when(metaData.getColumnTypeName(eq(0))).thenReturn("timestamptz");

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(resultSet.getObject(eq(0), eq(OffsetDateTime.class))).thenReturn(offsetDateTime);

    Schema.Field field1 = Schema.Field.of("field1", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    Schema schema = Schema.recordOf(
      "dbRecord",
      field1
    );
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    RedshiftDBRecord dbRecord = new RedshiftDBRecord();
    dbRecord.handleField(resultSet, builder, field1, 0, Types.TIMESTAMP, 0, 0);
    StructuredRecord record = builder.build();
    Assert.assertNotNull(record);
    Assert.assertNotNull(record.getTimestamp("field1", ZoneId.of("UTC")));
    Assert.assertEquals(record.getTimestamp("field1", ZoneId.of("UTC")).toInstant(), offsetDateTime.toInstant());
  }
}

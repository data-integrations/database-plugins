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

package io.cdap.plugin.oracle;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit Test class for the OracleSourceDBRecord
 */
@RunWith(MockitoJUnitRunner.class)
public class OracleSourceDBRecordUnitTest {

  private static final int DEFAULT_PRECISION = 38;

  @Mock
  ResultSet resultSet;

  @Mock
  ResultSetMetaData resultSetMetaData;

  /**
   * Validate the precision less Numbers handling against following use cases.
   * 1. Ensure that for Number(0,-127) non nullable type a String type is returned if output schema is String.
   * 2. Ensure that for Number(0,-127) non nullable type a String type is returned if output schema is String.
   * 3. Ensure that for Number(0,-127) nullable type a String type is returned if output schema is String.
   * 4. Ensure that for Number(0,-127) nullable type a String type is returned if output schema is String.
   * @throws Exception
   */
  @Test
  public void validatePrecisionLessNumberParsingForOutputSchemaAsString() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.of(Schema.Type.STRING));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.of(Schema.Type.STRING));
    Schema.Field field3 = Schema.Field.of("ID3", Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema.Field field4 = Schema.Field.of("ID4", Schema.nullableOf(Schema.of(Schema.Type.STRING)));

    Schema schema = Schema.recordOf(
        "dbRecord",
        field1,
        field2,
        field3,
        field4
    );

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getString(eq(1))).thenReturn("123");
    when(resultSet.getString(eq(2))).thenReturn("123.4568");
    when(resultSet.getString(eq(3))).thenReturn("123");
    when(resultSet.getString(eq(4))).thenReturn("123.4568");

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field2, 2, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field3, 3, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field4, 4, Types.NUMERIC, 0, -127);

    StructuredRecord record = builder.build();
    Assert.assertTrue(record.get("ID1") instanceof String);
    Assert.assertEquals(record.get("ID1"), "123");
    Assert.assertTrue(record.get("ID2") instanceof String);
    Assert.assertEquals(record.get("ID2"), "123.4568");
    Assert.assertTrue(record.get("ID3") instanceof String);
    Assert.assertEquals(record.get("ID3"), "123");
    Assert.assertTrue(record.get("ID4") instanceof String);
    Assert.assertEquals(record.get("ID4"), "123.4568");
  }

  /**
   * Validate the precision less Numbers handling against following use cases.
   * 1. Ensure that for Number(0,-127) non nullable type a Decimal type is returned if output schema is Decimal.
   * 2. Ensure that for Number(0,-127) non nullable type a String is returned if output schema is Decimal.
   * 3. Ensure that for Number(0,-127) nullable type a String type is returned if output schema is Decimal.
   * 4. Ensure that for Number(0,-127) nullable type a String is returned if output schema is Decimal.
   * @throws Exception
   */
  @Test
  public void validatePrecisionLessNumberParsingForOutputSchemaAsDecimal() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.decimalOf(DEFAULT_PRECISION));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.decimalOf(DEFAULT_PRECISION, 4));
    Schema.Field field3 = Schema.Field.of("ID3", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION)));
    Schema.Field field4 = Schema.Field.of("ID4", Schema.decimalOf(DEFAULT_PRECISION, 4));

    Schema schema = Schema.recordOf(
      "dbRecord",
      field1,
      field2,
      field3,
      field4
    );

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getBigDecimal(eq(1), eq(0))).thenReturn(new BigDecimal("123"));
    when(resultSet.getBigDecimal(eq(2), eq(4))).thenReturn(new BigDecimal("123.4568"));
    when(resultSet.getBigDecimal(eq(3), eq(0))).thenReturn(new BigDecimal("123"));
    when(resultSet.getBigDecimal(eq(4), eq(4))).thenReturn(new BigDecimal("123.4568"));

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field2, 2, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field3, 3, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field4, 4, Types.NUMERIC, 0, -127);

    StructuredRecord record = builder.build();
    Assert.assertTrue(record.getDecimal("ID1") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID1").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID2") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID2").toPlainString(), "123.4568");
    Assert.assertTrue(record.getDecimal("ID3") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID3").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID4") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID4").toPlainString(), "123.4568");
  }

  /**
   * Validate the default precision Numbers handling against following use cases.
   * 1. Ensure that for Number(38, 0) non nullable type a Number(38,0) is returned if output schema is Decimal.
   * 2. Ensure that for Number(38, 4) non nullable type a Number(38,6) is returned if output schema is Decimal.
   * 3. Ensure that for Number(38, 0) nullable type a Number(38,0) is returned if output schema is Decimal.
   * 4. Ensure that for Number(38, 4) nullable type a Number(38,6) is returned if output schema is Decimal.
   * @throws Exception
   */
  @Test
  public void validateDefaultDecimalParsing() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.decimalOf(DEFAULT_PRECISION));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.decimalOf(DEFAULT_PRECISION, 6));
    Schema.Field field3 = Schema.Field.of("ID3", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION)));
    Schema.Field field4 = Schema.Field.of("ID4", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION, 6)));

    Schema schema = Schema.recordOf(
        "dbRecord",
        field1,
        field2,
        field3,
        field4
    );

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getBigDecimal(eq(1), eq(0))).thenReturn(new BigDecimal("123"));
    when(resultSet.getBigDecimal(eq(2), eq(6))).thenReturn(new BigDecimal("123.456789"));
    when(resultSet.getBigDecimal(eq(3), eq(0))).thenReturn(new BigDecimal("123"));
    when(resultSet.getBigDecimal(eq(4), eq(6))).thenReturn(new BigDecimal("123.456789"));

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, Types.NUMERIC, DEFAULT_PRECISION, 0);
    dbRecord.handleField(resultSet, builder, field2, 2, Types.NUMERIC, DEFAULT_PRECISION, 4);
    dbRecord.handleField(resultSet, builder, field3, 3, Types.NUMERIC, DEFAULT_PRECISION, 0);
    dbRecord.handleField(resultSet, builder, field4, 4, Types.NUMERIC, DEFAULT_PRECISION, 4);

    StructuredRecord record = builder.build();
    Assert.assertTrue(record.getDecimal("ID1") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID1").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID2") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID2").toPlainString(), "123.456789");
    Assert.assertTrue(record.getDecimal("ID3") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID3").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID4") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID4").toPlainString(), "123.456789");
  }

  /**
   * Validate the Null value for TimestampLTZ datatype.
   * @throws Exception
   */
  @Test
  public void validateTimestampLTZTypeNullHandling() throws Exception {
    Schema.Field field1 = Schema.Field.of("field1", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    Schema schema = Schema.recordOf(
            "dbRecord",
            field1
    );

    when(resultSet.getTimestamp(eq(1))).thenReturn(null);

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, OracleSourceSchemaReader.TIMESTAMP_LTZ, DEFAULT_PRECISION, 0);

    StructuredRecord record = builder.build();
    Assert.assertNull(record.getTimestamp("field1"));
  }

  /***
   * Validate the TimestampTZ type handling in the OracleSourceDBRecord code
   * @throws Exception
   */
  @Test
  public void validateTimestampTZTypeNullHandling() throws Exception {
    Schema.Field field1 = Schema.Field.of("field1", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    Schema schema = Schema.recordOf(
            "dbRecord",
            field1
    );

    when(resultSet.getObject(eq(1))).thenReturn(null);

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, OracleSourceSchemaReader.TIMESTAMP_TZ, DEFAULT_PRECISION, 0);

    StructuredRecord record = builder.build();
    Assert.assertNull(record.get("field1"));
  }
}

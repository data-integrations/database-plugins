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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit Test class for the PostgresDBRecord
 */
@RunWith(MockitoJUnitRunner.class)
public class PostgresDBRecordUnitTest {

  private static final int DEFAULT_PRECISION = 38;

  /**
   * Validate the precision less Numbers handling against following use cases.
   * 1. Ensure that the numeric type with [p,s] set as [38,4] detect as BigDecimal(38,4) in cdap.
   * 2. Ensure that the numeric type without [p,s] detect as String type in cdap.
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
    when(resultSetMetaData.getColumnType(eq(1))).thenReturn(Types.NUMERIC);
    when(resultSetMetaData.getPrecision(eq(1))).thenReturn(DEFAULT_PRECISION);
    when(resultSetMetaData.getColumnType(eq(2))).thenReturn(Types.NUMERIC);
    when(resultSetMetaData.getPrecision(eq(2))).thenReturn(0);

    ResultSet resultSet = Mockito.mock(ResultSet.class);

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getBigDecimal(eq(1))).thenReturn(BigDecimal.valueOf(123.4568));
    when(resultSet.getString(eq(2))).thenReturn("123.4568");

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    PostgresDBRecord dbRecord = new PostgresDBRecord(null, null, null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, Types.NUMERIC, DEFAULT_PRECISION, 4);
    dbRecord.handleField(resultSet, builder, field2, 2, Types.NUMERIC, 0, -127);

    StructuredRecord record = builder.build();
    Assert.assertTrue(record.getDecimal("ID1") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID1"), BigDecimal.valueOf(123.4568));
    Assert.assertTrue(record.get("ID2") instanceof String);
    Assert.assertEquals(record.get("ID2"), "123.4568");
  }
}

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
import java.sql.Types;

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
   * 1. Ensure that for Number(0,-127) non nullable type a Number(38,0) is returned by default.
   * 2. Ensure that for Number(0,-127) non nullable type a Number(38,4) is returned,
   *    if schema defined this as Number(38,4).
   * 3. Ensure that for Number(0,-127) nullable type a Number(38,0) is returned by default.
   * 4. Ensure that for Number(0,-127) nullable type a Number(38,4) is returned,
   *    if schema defined this as Number(38,4).
   * @throws Exception
   */
  @Test
  public void validatePrecisionLessDecimalParsing() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.decimalOf(DEFAULT_PRECISION));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.decimalOf(DEFAULT_PRECISION, 4));
    Schema.Field field3 = Schema.Field.of("ID3", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION)));
    Schema.Field field4 = Schema.Field.of("ID4", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION, 4)));

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
    Assert.assertEquals(record.getDecimal("ID1").toPlainString(), "123");
    Assert.assertEquals(record.getDecimal("ID2").toPlainString(), "123.4568");
    Assert.assertEquals(record.getDecimal("ID3").toPlainString(), "123");
    Assert.assertEquals(record.getDecimal("ID4").toPlainString(), "123.4568");
  }
}

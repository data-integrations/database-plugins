/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.db;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CommonSchemaReaderTest {

  CommonSchemaReader reader;

  @Mock
  ResultSetMetaData metadata;

  @Before
  public void before() {
    reader = new CommonSchemaReader();
  }

  @Test
  public void testGetSchemaHandlesNull() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.NULL);
    Assert.assertEquals(Schema.of(Schema.Type.NULL), reader.getSchema(metadata, 1));
  }

  @Test
  public void testGetSchemaHandlesRowID() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.ROWID);
    Assert.assertEquals(Schema.of(Schema.Type.STRING), reader.getSchema(metadata, 1));
  }

  @Test
  public void testGetSchemaHandlesBoolean() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.BOOLEAN);
    Assert.assertEquals(Schema.of(Schema.Type.BOOLEAN), reader.getSchema(metadata, 1));

    when(metadata.getColumnType(eq(2))).thenReturn(Types.BIT);
    Assert.assertEquals(Schema.of(Schema.Type.BOOLEAN), reader.getSchema(metadata, 2));
  }

  @Test
  public void testGetSchemaHandlesInt() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.TINYINT);
    Assert.assertEquals(Schema.of(Schema.Type.INT), reader.getSchema(metadata, 1));

    when(metadata.getColumnType(eq(2))).thenReturn(Types.SMALLINT);
    Assert.assertEquals(Schema.of(Schema.Type.INT), reader.getSchema(metadata, 2));

    when(metadata.getColumnType(eq(3))).thenReturn(Types.INTEGER);
    when(metadata.isSigned(eq(3))).thenReturn(true);
    Assert.assertEquals(Schema.of(Schema.Type.INT), reader.getSchema(metadata, 3));
    when(metadata.getColumnType(eq(3))).thenReturn(Types.INTEGER);
    when(metadata.isSigned(eq(3))).thenReturn(false);
    Assert.assertEquals(Schema.of(Schema.Type.LONG), reader.getSchema(metadata, 3));
  }

  @Test
  public void testGetSchemaHandlesLong() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.BIGINT);
    when(metadata.isSigned(eq(1))).thenReturn(true);
    Assert.assertEquals(Schema.of(Schema.Type.LONG), reader.getSchema(metadata, 1));

    when(metadata.getColumnType(eq(1))).thenReturn(Types.BIGINT);
    when(metadata.isSigned(eq(1))).thenReturn(false);
    when(metadata.getPrecision(eq(1))).thenReturn(19);
    Assert.assertEquals(Schema.decimalOf(19), reader.getSchema(metadata, 1));
  }

  @Test
  public void testGetSchemaHandlesFloat() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.REAL);
    Assert.assertEquals(Schema.of(Schema.Type.FLOAT), reader.getSchema(metadata, 1));

    when(metadata.getColumnType(eq(2))).thenReturn(Types.FLOAT);
    Assert.assertEquals(Schema.of(Schema.Type.FLOAT), reader.getSchema(metadata, 2));
  }

  @Test
  public void testGetSchemaHandlesNumeric() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.NUMERIC);
    when(metadata.getPrecision(eq(1))).thenReturn(10);
    when(metadata.getScale(eq(1))).thenReturn(0);
    Assert.assertEquals(Schema.decimalOf(10, 0), reader.getSchema(metadata, 1));

    when(metadata.getColumnType(eq(2))).thenReturn(Types.DECIMAL);
    when(metadata.getPrecision(eq(2))).thenReturn(10);
    when(metadata.getScale(eq(2))).thenReturn(1);
    Assert.assertEquals(Schema.decimalOf(10, 1), reader.getSchema(metadata, 2));
  }

  @Test
  public void testGetSchemaHandlesDouble() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.DOUBLE);
    Assert.assertEquals(Schema.of(Schema.Type.DOUBLE), reader.getSchema(metadata, 1));
  }

  @Test
  public void testGetSchemaHandlesDate() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.DATE);
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATE), reader.getSchema(metadata, 1));
  }

  @Test
  public void testGetSchemaHandlesTime() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.TIME);
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIME_MICROS), reader.getSchema(metadata, 1));
  }

  @Test
  public void testGetSchemaHandlesTimestamp() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.TIMESTAMP);
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS), reader.getSchema(metadata, 1));
  }

  @Test
  public void testGetSchemaHandlesBytes() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.BINARY);
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), reader.getSchema(metadata, 1));

    when(metadata.getColumnType(eq(2))).thenReturn(Types.VARBINARY);
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), reader.getSchema(metadata, 2));

    when(metadata.getColumnType(eq(3))).thenReturn(Types.LONGVARBINARY);
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), reader.getSchema(metadata, 3));

    when(metadata.getColumnType(eq(4))).thenReturn(Types.BLOB);
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), reader.getSchema(metadata, 4));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSchemaThrowsExceptionOnNumericWithZeroPrecision() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.NUMERIC);
    when(metadata.getPrecision(eq(1))).thenReturn(0);
    when(metadata.getScale(eq(1))).thenReturn(10);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnArray() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.ARRAY);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnDatalink() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.DATALINK);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnDistinct() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.DISTINCT);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnJavaObject() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.JAVA_OBJECT);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnOther() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.OTHER);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnRef() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.REF);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnSQLXML() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.SQLXML);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = SQLException.class)
  public void testGetSchemaThrowsExceptionOnStruct() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.STRUCT);
    reader.getSchema(metadata, 1);
  }
}

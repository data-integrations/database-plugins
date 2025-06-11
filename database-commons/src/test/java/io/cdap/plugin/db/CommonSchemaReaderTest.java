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
import io.cdap.cdap.api.exception.ProgramFailureException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CommonSchemaReaderTest {

  CommonSchemaReader reader;

  @Mock
  ResultSetMetaData metadata;

  @Mock
  Connection mockConn;
  @Mock
  DatabaseMetaData mockDbMeta;
  @Mock
  ResultSet mockColumns;
  @Mock
  ResultSet mockTables;


  @Before
  public void before() {
    reader = new CommonSchemaReader() {
      @Override
      public Schema getSchema(String typeName, int columnType, int precision, int scale, String columnName,
                              boolean isSigned, boolean handleAsDecimal) {
        if ("INTEGER".equalsIgnoreCase(typeName) || columnType == Types.INTEGER) {
          return Schema.of(Schema.Type.INT);
        }
        if ("VARCHAR".equalsIgnoreCase(typeName) || columnType == Types.VARCHAR) {
          return Schema.of(Schema.Type.STRING);
        }
        if ("BIGINT".equalsIgnoreCase(typeName) || columnType == Types.BIGINT) {
          return Schema.of(Schema.Type.LONG);
        }
        return Schema.of(Schema.Type.STRING);
      }
    };
  }

  /**
   * Test: getSchemaFields(Connection, String) with a simple table name.
   * This covers the case where the table exists, and two columns are present:
   * one NOT NULL integer, one nullable string.
   */
  @Test
  public void testGetSchemaFieldsWithConnection() throws Exception {
    when(mockConn.getMetaData()).thenReturn(mockDbMeta);

    when(mockDbMeta.getColumns(any(), any(), eq("MYTABLE"), any())).thenReturn(mockColumns);
    when(mockColumns.next()).thenReturn(true, true, false);
    when(mockColumns.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(mockColumns.getString("TYPE_NAME")).thenReturn("INTEGER", "VARCHAR");
    when(mockColumns.getInt("DATA_TYPE")).thenReturn(Types.INTEGER, Types.VARCHAR);
    when(mockColumns.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(mockColumns.getInt("DECIMAL_DIGITS")).thenReturn(0, 0);
    when(mockColumns.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);

    java.util.List<Schema.Field> fields = reader.getSchemaFields(mockConn, "MYTABLE");

    Assert.assertEquals(2, fields.size());
    Assert.assertEquals("id", fields.get(0).getName());
    Assert.assertEquals(Schema.of(Schema.Type.INT), fields.get(0).getSchema());
    Assert.assertEquals("name", fields.get(1).getName());
    Assert.assertTrue(fields.get(1).getSchema().isNullable());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), fields.get(1).getSchema().getNonNullable());
  }

  /**
   * Test: getSchemaFields(Connection, String) with a schema-qualified table name.
   * This checks that "myschema.MYTABLE" is parsed and resolved correctly.
   */
  @Test
  public void testGetSchemaFieldsWithSchemaQualifiedName() throws Exception {
    // Setup for schema-qualified table name "myschema.MYTABLE"
    when(mockConn.getMetaData()).thenReturn(mockDbMeta);

    when(mockDbMeta.getColumns(any(), eq("myschema"), eq("MYTABLE"), any())).thenReturn(mockColumns);
    when(mockColumns.next()).thenReturn(true, false);
    when(mockColumns.getString("COLUMN_NAME")).thenReturn("id");
    when(mockColumns.getString("TYPE_NAME")).thenReturn("INTEGER");
    when(mockColumns.getInt("DATA_TYPE")).thenReturn(Types.INTEGER);
    when(mockColumns.getInt("COLUMN_SIZE")).thenReturn(10);
    when(mockColumns.getInt("DECIMAL_DIGITS")).thenReturn(0);
    when(mockColumns.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);

    java.util.List<Schema.Field> fields = reader.getSchemaFields(mockConn, "myschema.MYTABLE");
    Assert.assertEquals(1, fields.size());
    Assert.assertEquals("id", fields.get(0).getName());
    Assert.assertEquals(Schema.of(Schema.Type.INT), fields.get(0).getSchema());
  }

  /**
   * Test: Nullability logic is correct for columns.
   */
  @Test
  public void testGetSchemaFieldsHandlesNullability() throws Exception {
    when(mockConn.getMetaData()).thenReturn(mockDbMeta);
    when(mockDbMeta.getColumns(any(), any(), eq("MYTABLE"), any())).thenReturn(mockColumns);
    when(mockColumns.next()).thenReturn(true, true, false);
    when(mockColumns.getString("COLUMN_NAME")).thenReturn("col1", "col2");
    when(mockColumns.getString("TYPE_NAME")).thenReturn("INTEGER", "VARCHAR");
    when(mockColumns.getInt("DATA_TYPE")).thenReturn(Types.INTEGER, Types.VARCHAR);
    when(mockColumns.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(mockColumns.getInt("DECIMAL_DIGITS")).thenReturn(0, 0);
    when(mockColumns.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNullable, DatabaseMetaData.columnNoNulls);

    java.util.List<Schema.Field> fields = reader.getSchemaFields(mockConn, "MYTABLE");
    Assert.assertTrue(fields.get(0).getSchema().isNullable());
    Assert.assertFalse(fields.get(1).getSchema().isNullable());
  }

  /**
   * Test: Exception is thrown when table is not found.
   */
  @Test(expected = SQLException.class)
  public void testGetSchemaFieldsThrowsWhenTableNotFound() throws Exception {
    when(mockConn.getMetaData()).thenReturn(mockDbMeta);
    when(mockDbMeta.getColumns(any(), any(), eq("NOTABLE"), any())).thenReturn(mockColumns);
    when(mockColumns.next()).thenReturn(false); // No columns found

    reader.getSchemaFields(mockConn, "NOTABLE");
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

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnArray() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.ARRAY);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnDatalink() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.DATALINK);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnDistinct() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.DISTINCT);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnJavaObject() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.JAVA_OBJECT);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnOther() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.OTHER);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnRef() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.REF);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnSQLXML() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.SQLXML);
    reader.getSchema(metadata, 1);
  }

  @Test(expected = ProgramFailureException.class)
  public void testGetSchemaThrowsExceptionOnStruct() throws SQLException {
    when(metadata.getColumnType(eq(1))).thenReturn(Types.STRUCT);
    reader.getSchema(metadata, 1);
  }
}

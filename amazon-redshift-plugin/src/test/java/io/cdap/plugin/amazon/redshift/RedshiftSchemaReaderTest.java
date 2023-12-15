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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class RedshiftSchemaReaderTest {

  @Test
  public void testGetSchema() throws SQLException {
    RedshiftSchemaReader schemaReader = new RedshiftSchemaReader();

    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("timetz");
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.TIMESTAMP);

    Schema schema = schemaReader.getSchema(metadata, 1);

    Assert.assertEquals(Schema.of(Schema.Type.STRING), schema);
  }

  @Test
  public void testGetSchemaWithIntType() throws SQLException {
    RedshiftSchemaReader schemaReader = new RedshiftSchemaReader();
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("INT");
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.NUMERIC);
    Schema schema = schemaReader.getSchema(metadata, 1);

    Assert.assertEquals(Schema.of(Schema.Type.INT), schema);
  }

  @Test
  public void testGetSchemaWithNumericTypeWithPrecision() throws SQLException {
    RedshiftSchemaReader schemaReader = new RedshiftSchemaReader();
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("STRING");
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.NUMERIC);
    Mockito.when(metadata.getPrecision(1)).thenReturn(0);

    Schema schema = schemaReader.getSchema(metadata, 1);

    Assert.assertEquals(Schema.of(Schema.Type.STRING), schema);
  }

  @Test
  public void testGetSchemaWithOtherTypes() throws SQLException {
    RedshiftSchemaReader schemaReader = new RedshiftSchemaReader();
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("BIGINT");
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.BIGINT);
    Schema schema = schemaReader.getSchema(metadata, 1);

    Assert.assertEquals(Schema.of(Schema.Type.LONG), schema);

    Mockito.when(metadata.getColumnTypeName(2)).thenReturn("timestamp");
    Mockito.when(metadata.getColumnType(2)).thenReturn(Types.TIMESTAMP);

    schema = schemaReader.getSchema(metadata, 2);

    Assert.assertEquals(Schema.of(Schema.LogicalType.DATETIME), schema);
  }

  @Test
  public void testShouldIgnoreColumn() throws SQLException {
    RedshiftSchemaReader schemaReader = new RedshiftSchemaReader("sessionID");
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(metadata.getColumnName(1)).thenReturn("c_sessionID");
    Assert.assertTrue(schemaReader.shouldIgnoreColumn(metadata, 1));
    Mockito.when(metadata.getColumnName(2)).thenReturn("sqn_sessionID");
    Assert.assertTrue(schemaReader.shouldIgnoreColumn(metadata, 2));
    Mockito.when(metadata.getColumnName(3)).thenReturn("columnName");
    Assert.assertFalse(schemaReader.shouldIgnoreColumn(metadata, 3));
  }

  @Test
  public void testGetSchemaFields() throws SQLException {
    RedshiftSchemaReader schemaReader = new RedshiftSchemaReader();

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);

    // Mock two columns with different types
    Mockito.when(metadata.getColumnCount()).thenReturn(2);
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("INT");
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.NUMERIC);
    Mockito.when(metadata.getColumnName(1)).thenReturn("column1");

    Mockito.when(metadata.getColumnTypeName(2)).thenReturn("BIGINT");
    Mockito.when(metadata.getColumnType(2)).thenReturn(Types.BIGINT);
    Mockito.when(metadata.getColumnName(2)).thenReturn("column2");

    List<Schema.Field> expectedSchemaFields = Lists.newArrayList();
    expectedSchemaFields.add(Schema.Field.of("column1", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    expectedSchemaFields.add(Schema.Field.of("column2", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

    List<Schema.Field> actualSchemaFields = schemaReader.getSchemaFields(resultSet);

    Assert.assertEquals(expectedSchemaFields.get(0).getName(), actualSchemaFields.get(0).getName());
    Assert.assertEquals(expectedSchemaFields.get(1).getName(), actualSchemaFields.get(1).getName());
  }
}

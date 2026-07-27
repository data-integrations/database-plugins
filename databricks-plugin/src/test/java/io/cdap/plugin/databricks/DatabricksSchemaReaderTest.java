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

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class DatabricksSchemaReaderTest {

  private void mockColumnType(ResultSetMetaData metadata, int index, String typeName, int sqlType) throws SQLException {
    Mockito.when(metadata.getColumnTypeName(index)).thenReturn(typeName);
    Mockito.when(metadata.getColumnType(index)).thenReturn(sqlType);
    Mockito.when(metadata.isSigned(index)).thenReturn(true);
  }

  @Test
  public void testGetSchemaDatabricksTypes() throws SQLException {
    DatabricksSchemaReader schemaReader = new DatabricksSchemaReader();
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);

    mockColumnType(metadata, 1, "INT", Types.INTEGER);
    mockColumnType(metadata, 2, "BIGINT", Types.BIGINT);
    mockColumnType(metadata, 3, "TIMESTAMP", Types.TIMESTAMP);
    mockColumnType(metadata, 4, "TIMESTAMP_NTZ", Types.TIMESTAMP);
    mockColumnType(metadata, 5, "DATE", Types.DATE);
    mockColumnType(metadata, 6, "VARIANT", Types.OTHER);
    mockColumnType(metadata, 7, "STRUCT", Types.STRUCT);
    mockColumnType(metadata, 8, "ARRAY", Types.ARRAY);
    mockColumnType(metadata, 9, "MAP", Types.OTHER);
    mockColumnType(metadata, 10, "SMALLINT", Types.SMALLINT);
    mockColumnType(metadata, 11, "TINYINT", Types.TINYINT);
    mockColumnType(metadata, 12, "TIME", Types.TIME);
    mockColumnType(metadata, 13, "INTERVAL", Types.OTHER);
    mockColumnType(metadata, 14, "VOID", Types.NULL);
    mockColumnType(metadata, 15, "GEOGRAPHY", Types.OTHER);
    mockColumnType(metadata, 16, "GEOMETRY", Types.OTHER);
    mockColumnType(metadata, 17, "FILE", Types.OTHER);
    mockColumnType(metadata, 18, "OBJECT", Types.OTHER);

    Assert.assertEquals(Schema.of(Schema.Type.INT), schemaReader.getSchema(metadata, 1));
    Assert.assertEquals(Schema.of(Schema.Type.LONG), schemaReader.getSchema(metadata, 2));
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS), schemaReader.getSchema(metadata, 3));
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATETIME), schemaReader.getSchema(metadata, 4));
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATE), schemaReader.getSchema(metadata, 5));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 6));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 7));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 8));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 9));
    Assert.assertEquals(Schema.of(Schema.Type.INT), schemaReader.getSchema(metadata, 10));
    Assert.assertEquals(Schema.of(Schema.Type.INT), schemaReader.getSchema(metadata, 11));
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIME_MICROS), schemaReader.getSchema(metadata, 12));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 13));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 14));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 15));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 16));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 17));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 18));
  }

  @Test
  public void testShouldIgnoreColumn() throws SQLException {
    DatabricksSchemaReader schemaReader = new DatabricksSchemaReader("sessionID");
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(metadata.getColumnName(1)).thenReturn("c_sessionID");
    Mockito.when(metadata.getColumnName(2)).thenReturn("sqn_sessionID");
    Mockito.when(metadata.getColumnName(3)).thenReturn("columnName");

    Assert.assertTrue(schemaReader.shouldIgnoreColumn(metadata, 1));
    Assert.assertTrue(schemaReader.shouldIgnoreColumn(metadata, 2));
    Assert.assertFalse(schemaReader.shouldIgnoreColumn(metadata, 3));
  }
}

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

import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class DatabricksSchemaReaderTest {

  private ResultSetMetaData createMockMetadata(Map<Integer, String> columnTypeNames,
                                                Map<Integer, String> columnNames) {
    return (ResultSetMetaData) Proxy.newProxyInstance(
      ResultSetMetaData.class.getClassLoader(),
      new Class<?>[]{ResultSetMetaData.class},
      (proxy, method, args) -> {
        if ("getColumnTypeName".equals(method.getName())) {
          int index = (Integer) args[0];
          return columnTypeNames.get(index);
        }
        if ("getColumnName".equals(method.getName())) {
          int index = (Integer) args[0];
          return columnNames.get(index);
        }
        return null;
      }
    );
  }

  @Test
  public void testGetSchemaDatabricksTypes() throws SQLException {
    DatabricksSchemaReader schemaReader = new DatabricksSchemaReader();
    Map<Integer, String> typeNames = new java.util.HashMap<>();
    typeNames.put(1, "INT");
    typeNames.put(2, "BIGINT");
    typeNames.put(3, "TIMESTAMP");
    typeNames.put(4, "TIMESTAMP_NTZ");
    typeNames.put(5, "DATE");
    typeNames.put(6, "VARIANT");
    typeNames.put(7, "STRUCT");
    typeNames.put(8, "ARRAY");
    typeNames.put(9, "MAP");

    ResultSetMetaData metadata = createMockMetadata(typeNames, java.util.Collections.emptyMap());

    Assert.assertEquals(Schema.of(Schema.Type.INT), schemaReader.getSchema(metadata, 1));
    Assert.assertEquals(Schema.of(Schema.Type.LONG), schemaReader.getSchema(metadata, 2));
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATETIME), schemaReader.getSchema(metadata, 3));
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATETIME), schemaReader.getSchema(metadata, 4));
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATE), schemaReader.getSchema(metadata, 5));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 6));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 7));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 8));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), schemaReader.getSchema(metadata, 9));
  }

  @Test
  public void testShouldIgnoreColumn() throws SQLException {
    DatabricksSchemaReader schemaReader = new DatabricksSchemaReader("sessionID");
    Map<Integer, String> names = new java.util.HashMap<>();
    names.put(1, "c_sessionID");
    names.put(2, "sqn_sessionID");
    names.put(3, "columnName");

    ResultSetMetaData metadata = createMockMetadata(java.util.Collections.emptyMap(), names);

    Assert.assertTrue(schemaReader.shouldIgnoreColumn(metadata, 1));
    Assert.assertTrue(schemaReader.shouldIgnoreColumn(metadata, 2));
    Assert.assertFalse(schemaReader.shouldIgnoreColumn(metadata, 3));
  }
}

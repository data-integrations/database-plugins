/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.ProgramFailureException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

public class OracleSchemaReaderTest {

  @Test
  public void getSchema_timestampLTZFieldTrue_returnTimestamp() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader(null, false, false, true, false);

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Statement statement = Mockito.mock(Statement.class);
    Connection connection = Mockito.mock(Connection.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);

    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);

    Mockito.when(metadata.getColumnCount()).thenReturn(2);
    // -101 is for TIMESTAMP_TZ
    Mockito.when(metadata.getColumnType(1)).thenReturn(-101);
    Mockito.when(metadata.getColumnName(1)).thenReturn("column1");

    // -102 is for TIMESTAMP_LTZ
    Mockito.when(metadata.getColumnType(2)).thenReturn(-102);
    Mockito.when(metadata.getColumnName(2)).thenReturn("column2");

    List<Schema.Field> expectedSchemaFields = Lists.newArrayList();
    expectedSchemaFields.add(Schema.Field.of("column1", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
    expectedSchemaFields.add(Schema.Field.of("column2", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    List<Schema.Field> actualSchemaFields = schemaReader.getSchemaFields(resultSet);

    Assert.assertEquals(expectedSchemaFields.get(0).getName(), actualSchemaFields.get(0).getName());
    Assert.assertEquals(expectedSchemaFields.get(0).getSchema(), actualSchemaFields.get(0).getSchema());
    Assert.assertEquals(expectedSchemaFields.get(1).getName(), actualSchemaFields.get(1).getName());
    Assert.assertEquals(expectedSchemaFields.get(1).getSchema(), actualSchemaFields.get(1).getSchema());

  }

  @Test
  public void getSchema_timestampLTZFieldFalse_returnDatetime() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader(null, false, false, false, false);

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Statement statement = Mockito.mock(Statement.class);
    Connection connection = Mockito.mock(Connection.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);
    Mockito.when(metadata.getColumnCount()).thenReturn(2);
    // -101 is for TIMESTAMP_TZ
    Mockito.when(metadata.getColumnType(1)).thenReturn(-101);
    Mockito.when(metadata.getColumnName(1)).thenReturn("column1");

    // -102 is for TIMESTAMP_LTZ
    Mockito.when(metadata.getColumnType(2)).thenReturn(-102);
    Mockito.when(metadata.getColumnName(2)).thenReturn("column2");

    List<Schema.Field> expectedSchemaFields = Lists.newArrayList();
    expectedSchemaFields.add(Schema.Field.of("column1", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
    expectedSchemaFields.add(Schema.Field.of("column2", Schema.of(Schema.LogicalType.DATETIME)));

    List<Schema.Field> actualSchemaFields = schemaReader.getSchemaFields(resultSet);

    Assert.assertEquals(expectedSchemaFields.get(0).getName(), actualSchemaFields.get(0).getName());
    Assert.assertEquals(expectedSchemaFields.get(0).getSchema(), actualSchemaFields.get(0).getSchema());
    Assert.assertEquals(expectedSchemaFields.get(1).getName(), actualSchemaFields.get(1).getName());
    Assert.assertEquals(expectedSchemaFields.get(1).getSchema(), actualSchemaFields.get(1).getSchema());
  }

  @Test
  public void getSchemaFields_structType_returnRecord() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader();
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Statement statement = Mockito.mock(Statement.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
    ResultSet attrRs = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(stmt);
    Mockito.when(stmt.executeQuery()).thenReturn(attrRs);
    Mockito.when(metadata.getColumnCount()).thenReturn(1);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.STRUCT);
    Mockito.when(metadata.getColumnName(1)).thenReturn("address");
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("CS_ITN.ADDRESS_TYPE");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST_SCHEMA");
    Mockito.when(attrRs.next()).thenReturn(true, true, false);
    Mockito.when(attrRs.getString("ATTR_NAME")).thenReturn("STREET", "CITY");
    Mockito.when(attrRs.getString("ATTR_TYPE_NAME")).thenReturn("VARCHAR2", "VARCHAR2");
    Mockito.when(attrRs.getInt("PRECISION")).thenReturn(0, 0);
    Mockito.when(attrRs.getInt("SCALE")).thenReturn(0, 0);

    List<Schema.Field> actualFields = schemaReader.getSchemaFields(resultSet);

    Schema.Field addressField = actualFields.get(0);
    Schema addressSchema = addressField.getSchema().isNullable()
            ? addressField.getSchema().getNonNullable() : addressField.getSchema();
    List<Schema.Field> structFields = addressSchema.getFields();
    Assert.assertEquals(1, actualFields.size());
    Assert.assertEquals("address", addressField.getName());
    Assert.assertEquals(Schema.Type.RECORD, addressSchema.getType());
    Assert.assertEquals("CS_ITN.ADDRESS_TYPE", addressSchema.getRecordName());
    Assert.assertEquals(2, structFields.size());
    Assert.assertEquals("STREET", structFields.get(0).getName());
    Assert.assertEquals("CITY", structFields.get(1).getName());
  }

  @Test
  public void getSchema_xmlField_returnString() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader(null, false, false, false, true);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(metadata.getColumnCount()).thenReturn(1);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.SQLXML);
    Mockito.when(metadata.getColumnName(1)).thenReturn("xmlData");
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);

    List<Schema.Field> actualSchemaFields = schemaReader.getSchemaFields(resultSet);

    List<Schema.Field> expectedSchemaFields = Lists.newArrayList();
    expectedSchemaFields.add(Schema.Field.of("xmlData", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expectedSchemaFields.get(0).getName(), actualSchemaFields.get(0).getName());
    Assert.assertEquals(expectedSchemaFields.get(0).getSchema(), actualSchemaFields.get(0).getSchema());
  }

  @Test
  public void getSchema_xmlFieldDisabled_throwsProgramFailureException() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader(null,
            false, false, false, false);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(metadata.getColumnCount()).thenReturn(1);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.SQLXML);
    Mockito.when(metadata.getColumnName(1)).thenReturn("xmlData");
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);

    Assert.assertThrows(ProgramFailureException.class, () -> schemaReader.getSchemaFields(resultSet));

  }

  @Test
  public void getSchemaFields_structWithUnsupportedAttributeType_throwsException() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader();
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Statement statement = Mockito.mock(Statement.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
    ResultSet attrRs = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(stmt);
    Mockito.when(stmt.executeQuery()).thenReturn(attrRs);
    Mockito.when(metadata.getColumnCount()).thenReturn(1);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.STRUCT);
    Mockito.when(metadata.getColumnName(1)).thenReturn("complex_payload");
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("CS_ITN.ANYDATA_TYPE");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST_SCHEMA");
    Mockito.when(attrRs.next()).thenReturn(true, true, false);
    Mockito.when(attrRs.getString("ATTR_NAME")).thenReturn("VALID_ID", "UNSUPPORTED_DATA");
    Mockito.when(attrRs.getString("ATTR_TYPE_NAME")).thenReturn("NUMBER", "ANYDATA");
    Mockito.when(attrRs.getInt("PRECISION")).thenReturn(10, 0);
    Mockito.when(attrRs.getInt("SCALE")).thenReturn(0, 0);

    Assert.assertThrows(ProgramFailureException.class, () -> schemaReader.getSchemaFields(resultSet));

  }
}

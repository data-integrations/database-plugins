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
import java.util.Arrays;
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

    Boolean[] nextReturns = new Boolean[31];
    Arrays.fill(nextReturns, 0, 30, true);
    nextReturns[30] = false;
    Mockito.when(attrRs.next()).thenReturn(true, Arrays.copyOfRange(nextReturns, 1, 31));

    Mockito.when(attrRs.getString("ATTR_NAME")).thenReturn(
            "ATTR_VARCHAR2", "ATTR_VARCHAR", "ATTR_CHAR", "ATTR_CHAR2", "ATTR_NCHAR",
            "ATTR_NVARCHAR2", "ATTR_CLOB", "ATTR_NCLOB", "ATTR_LONG", "ATTR_ROWID",
            "ATTR_UROWID", "ATTR_NUMBER_PREC", "ATTR_NUMBER_NOPREC", "ATTR_DECIMAL",
            "ATTR_INTEGER", "ATTR_FLOAT", "ATTR_REAL", "ATTR_DOUBLE", "ATTR_BINARY_FLOAT",
            "ATTR_BINARY_DOUBLE", "ATTR_DATE", "ATTR_TIMESTAMP", "ATTR_TIMESTAMP_TZ",
            "ATTR_TIMESTAMP_LTZ", "ATTR_INTERVAL_DS", "ATTR_INTERVAL_YM", "ATTR_BLOB",
            "ATTR_RAW", "ATTR_LONG_RAW", "ATTR_BFILE"
    );
    Mockito.when(attrRs.getString("ATTR_TYPE_NAME")).thenReturn(
            "VARCHAR2", "VARCHAR", "CHAR", "CHAR2", "NCHAR",
            "NVARCHAR2", "CLOB", "NCLOB", "LONG", "ROWID",
            "UROWID", "NUMBER", "NUMBER", "DECIMAL",
            "INTEGER", "FLOAT", "REAL", "DOUBLE", "BINARY_FLOAT",
            "BINARY_DOUBLE", "DATE", "TIMESTAMP", "TIMESTAMP WITH TZ",
            "TIMESTAMP WITH LOCAL TZ", "INTERVAL DAY TO SECOND", "INTERVAL YEAR TO MONTH", "BLOB",
            "RAW", "LONG RAW", "BFILE"
    );
    Mockito.when(attrRs.getInt("PRECISION")).thenReturn(
            50, 50, 10, 10, 10,
            50, 0, 0, 0, 0,
            0, 10, 0, 8,
            10, 10, 10, 10, 0,
            0, 0, 0, 0,
            0, 0, 0, 0,
            100, 0, 0
    );
    Mockito.when(attrRs.getInt("SCALE")).thenReturn(
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 2, 0, 2,
            0, 0, 0, 0, 0,
            0, 0, 0, 0,
            0, 0, 0
    );

    List<Schema.Field> actualFields = schemaReader.getSchemaFields(resultSet);

    Schema.Field addressField = actualFields.get(0);
    Schema addressSchema = addressField.getSchema().isNullable()
            ? addressField.getSchema().getNonNullable() : addressField.getSchema();
    List<Schema.Field> structFields = addressSchema.getFields();
    Assert.assertEquals(1, actualFields.size());
    Assert.assertEquals("address", addressField.getName());
    Assert.assertEquals(Schema.Type.RECORD, addressSchema.getType());
    Assert.assertEquals("CS_ITN.ADDRESS_TYPE", addressSchema.getRecordName());
    Assert.assertEquals(30, structFields.size());

    Assert.assertEquals("ATTR_VARCHAR2", structFields.get(0).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(0).getSchema());
    Assert.assertEquals("ATTR_VARCHAR", structFields.get(1).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(1).getSchema());
    Assert.assertEquals("ATTR_CHAR", structFields.get(2).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(2).getSchema());
    Assert.assertEquals("ATTR_CHAR2", structFields.get(3).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(3).getSchema());
    Assert.assertEquals("ATTR_NCHAR", structFields.get(4).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(4).getSchema());
    Assert.assertEquals("ATTR_NVARCHAR2", structFields.get(5).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(5).getSchema());
    Assert.assertEquals("ATTR_CLOB", structFields.get(6).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(6).getSchema());
    Assert.assertEquals("ATTR_NCLOB", structFields.get(7).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(7).getSchema());
    Assert.assertEquals("ATTR_LONG", structFields.get(8).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(8).getSchema());
    Assert.assertEquals("ATTR_ROWID", structFields.get(9).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(9).getSchema());
    Assert.assertEquals("ATTR_UROWID", structFields.get(10).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(10).getSchema());
    Assert.assertEquals("ATTR_NUMBER_PREC", structFields.get(11).getName());
    Assert.assertEquals(Schema.decimalOf(10, 2), structFields.get(11).getSchema());
    Assert.assertEquals("ATTR_NUMBER_NOPREC", structFields.get(12).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(12).getSchema());
    Assert.assertEquals("ATTR_DECIMAL", structFields.get(13).getName());
    Assert.assertEquals(Schema.decimalOf(8, 2), structFields.get(13).getSchema());
    Assert.assertEquals("ATTR_INTEGER", structFields.get(14).getName());
    Assert.assertEquals(Schema.of(Schema.Type.INT), structFields.get(14).getSchema());
    Assert.assertEquals("ATTR_FLOAT", structFields.get(15).getName());
    Assert.assertEquals(Schema.of(Schema.Type.DOUBLE), structFields.get(15).getSchema());
    Assert.assertEquals("ATTR_REAL", structFields.get(16).getName());
    Assert.assertEquals(Schema.of(Schema.Type.DOUBLE), structFields.get(16).getSchema());
    Assert.assertEquals("ATTR_DOUBLE", structFields.get(17).getName());
    Assert.assertEquals(Schema.of(Schema.Type.DOUBLE), structFields.get(17).getSchema());
    Assert.assertEquals("ATTR_BINARY_FLOAT", structFields.get(18).getName());
    Assert.assertEquals(Schema.of(Schema.Type.FLOAT), structFields.get(18).getSchema());
    Assert.assertEquals("ATTR_BINARY_DOUBLE", structFields.get(19).getName());
    Assert.assertEquals(Schema.of(Schema.Type.DOUBLE), structFields.get(19).getSchema());
    Assert.assertEquals("ATTR_DATE", structFields.get(20).getName());
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATETIME), structFields.get(20).getSchema());
    Assert.assertEquals("ATTR_TIMESTAMP", structFields.get(21).getName());
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATETIME), structFields.get(21).getSchema());
    Assert.assertEquals("ATTR_TIMESTAMP_TZ", structFields.get(22).getName());
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS),
            structFields.get(22).getSchema());
    Assert.assertEquals("ATTR_TIMESTAMP_LTZ", structFields.get(23).getName());
    Assert.assertEquals(Schema.of(Schema.LogicalType.DATETIME), structFields.get(23).getSchema());
    Assert.assertEquals("ATTR_INTERVAL_DS", structFields.get(24).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(24).getSchema());
    Assert.assertEquals("ATTR_INTERVAL_YM", structFields.get(25).getName());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), structFields.get(25).getSchema());
    Assert.assertEquals("ATTR_BLOB", structFields.get(26).getName());
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), structFields.get(26).getSchema());
    Assert.assertEquals("ATTR_RAW", structFields.get(27).getName());
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), structFields.get(27).getSchema());
    Assert.assertEquals("ATTR_LONG_RAW", structFields.get(28).getName());
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), structFields.get(28).getSchema());
    Assert.assertEquals("ATTR_BFILE", structFields.get(29).getName());
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), structFields.get(29).getSchema());
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

  @Test
  public void getSchemaFields_validNestedStructLevel_returnRecord() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader();
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Statement statement = Mockito.mock(Statement.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement stmt0 = Mockito.mock(PreparedStatement.class);
    PreparedStatement stmt1 = Mockito.mock(PreparedStatement.class);
    PreparedStatement stmt2 = Mockito.mock(PreparedStatement.class);
    PreparedStatement stmt3 = Mockito.mock(PreparedStatement.class);
    ResultSet attrRs0 = Mockito.mock(ResultSet.class);
    ResultSet attrRs1 = Mockito.mock(ResultSet.class);
    ResultSet attrRs2 = Mockito.mock(ResultSet.class);
    ResultSet attrRs3 = Mockito.mock(ResultSet.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(Mockito.anyString()))
            .thenReturn(stmt0, stmt1, stmt2, stmt3);
    Mockito.when(stmt0.executeQuery()).thenReturn(attrRs0);
    Mockito.when(stmt1.executeQuery()).thenReturn(attrRs1);
    Mockito.when(stmt2.executeQuery()).thenReturn(attrRs2);
    Mockito.when(stmt3.executeQuery()).thenReturn(attrRs3);

    Mockito.when(metadata.getColumnCount()).thenReturn(1);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.STRUCT);
    Mockito.when(metadata.getColumnName(1)).thenReturn("payload");
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("TEST.STRUCT_L0");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST");

    Mockito.when(attrRs0.next()).thenReturn(true, false);
    Mockito.when(attrRs0.getString("ATTR_NAME")).thenReturn("SUB1");
    Mockito.when(attrRs0.getString("ATTR_TYPE_NAME")).thenReturn("STRUCT_L1");
    Mockito.when(attrRs0.getString("ATTR_TYPE_OWNER")).thenReturn("TEST");

    Mockito.when(attrRs1.next()).thenReturn(true, false);
    Mockito.when(attrRs1.getString("ATTR_NAME")).thenReturn("SUB2");
    Mockito.when(attrRs1.getString("ATTR_TYPE_NAME")).thenReturn("STRUCT_L2");
    Mockito.when(attrRs1.getString("ATTR_TYPE_OWNER")).thenReturn("TEST");

    Mockito.when(attrRs2.next()).thenReturn(true, false);
    Mockito.when(attrRs2.getString("ATTR_NAME")).thenReturn("SUB3");
    Mockito.when(attrRs2.getString("ATTR_TYPE_NAME")).thenReturn("STRUCT_L3");
    Mockito.when(attrRs2.getString("ATTR_TYPE_OWNER")).thenReturn("TEST");

    Mockito.when(attrRs3.next()).thenReturn(true, false);
    Mockito.when(attrRs3.getString("ATTR_NAME")).thenReturn("ID");
    Mockito.when(attrRs3.getString("ATTR_TYPE_NAME")).thenReturn("VARCHAR2");
    Mockito.when(attrRs3.getInt("PRECISION")).thenReturn(50);
    Mockito.when(attrRs3.getInt("SCALE")).thenReturn(0);

    List<Schema.Field> actualFields = schemaReader.getSchemaFields(resultSet);
    Assert.assertEquals(1, actualFields.size());
    Schema l0Schema = actualFields.get(0).getSchema().isNullable()
            ? actualFields.get(0).getSchema().getNonNullable() : actualFields.get(0).getSchema();
    Assert.assertEquals(Schema.Type.RECORD, l0Schema.getType());
    Schema l1Schema = l0Schema.getField("SUB1").getSchema().isNullable()
            ? l0Schema.getField("SUB1").getSchema().getNonNullable()
            : l0Schema.getField("SUB1").getSchema();
    Assert.assertEquals(Schema.Type.RECORD, l1Schema.getType());
    Schema l2Schema = l1Schema.getField("SUB2").getSchema().isNullable()
            ? l1Schema.getField("SUB2").getSchema().getNonNullable()
            : l1Schema.getField("SUB2").getSchema();
    Assert.assertEquals(Schema.Type.RECORD, l2Schema.getType());
    Schema l3Schema = l2Schema.getField("SUB3").getSchema().isNullable()
            ? l2Schema.getField("SUB3").getSchema().getNonNullable()
            : l2Schema.getField("SUB3").getSchema();
    Assert.assertEquals(Schema.Type.RECORD, l3Schema.getType());
    Assert.assertEquals(Schema.Type.STRING, l3Schema.getField("ID").getSchema().getType());
  }

  @Test
  public void getSchemaFields_exceedsMaxNestedStructLevel_throwsException() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader();
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    Statement statement = Mockito.mock(Statement.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement stmt0 = Mockito.mock(PreparedStatement.class);
    PreparedStatement stmt1 = Mockito.mock(PreparedStatement.class);
    PreparedStatement stmt2 = Mockito.mock(PreparedStatement.class);
    PreparedStatement stmt3 = Mockito.mock(PreparedStatement.class);
    ResultSet attrRs0 = Mockito.mock(ResultSet.class);
    ResultSet attrRs1 = Mockito.mock(ResultSet.class);
    ResultSet attrRs2 = Mockito.mock(ResultSet.class);
    ResultSet attrRs3 = Mockito.mock(ResultSet.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(Mockito.anyString()))
            .thenReturn(stmt0, stmt1, stmt2, stmt3);
    Mockito.when(stmt0.executeQuery()).thenReturn(attrRs0);
    Mockito.when(stmt1.executeQuery()).thenReturn(attrRs1);
    Mockito.when(stmt2.executeQuery()).thenReturn(attrRs2);
    Mockito.when(stmt3.executeQuery()).thenReturn(attrRs3);

    Mockito.when(metadata.getColumnCount()).thenReturn(1);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.STRUCT);
    Mockito.when(metadata.getColumnName(1)).thenReturn("payload");
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("TEST.STRUCT_L0");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST");

    Mockito.when(attrRs0.next()).thenReturn(true, false);
    Mockito.when(attrRs0.getString("ATTR_NAME")).thenReturn("SUB1");
    Mockito.when(attrRs0.getString("ATTR_TYPE_NAME")).thenReturn("STRUCT_L1");
    Mockito.when(attrRs0.getString("ATTR_TYPE_OWNER")).thenReturn("TEST");

    Mockito.when(attrRs1.next()).thenReturn(true, false);
    Mockito.when(attrRs1.getString("ATTR_NAME")).thenReturn("SUB2");
    Mockito.when(attrRs1.getString("ATTR_TYPE_NAME")).thenReturn("STRUCT_L2");
    Mockito.when(attrRs1.getString("ATTR_TYPE_OWNER")).thenReturn("TEST");

    Mockito.when(attrRs2.next()).thenReturn(true, false);
    Mockito.when(attrRs2.getString("ATTR_NAME")).thenReturn("SUB3");
    Mockito.when(attrRs2.getString("ATTR_TYPE_NAME")).thenReturn("STRUCT_L3");
    Mockito.when(attrRs2.getString("ATTR_TYPE_OWNER")).thenReturn("TEST");

    Mockito.when(attrRs3.next()).thenReturn(true, false);
    Mockito.when(attrRs3.getString("ATTR_NAME")).thenReturn("SUB4");
    Mockito.when(attrRs3.getString("ATTR_TYPE_NAME")).thenReturn("STRUCT_L4");
    Mockito.when(attrRs3.getString("ATTR_TYPE_OWNER")).thenReturn("TEST");

    Assert.assertThrows(IllegalArgumentException.class, () -> schemaReader.getSchemaFields(resultSet));
  }
}

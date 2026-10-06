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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public class OracleSchemaReaderTest {

  private ResultSet resultSet;
  private ResultSetMetaData metadata;
  private Statement statement;
  private Connection connection;
  private PreparedStatement preparedStatement;
  private ResultSet attributeResultSet;
  private OracleSourceSchemaReader defaultSchemaReader;

  @Before
  public void setUp() throws SQLException {
    resultSet = Mockito.mock(ResultSet.class);
    metadata = Mockito.mock(ResultSetMetaData.class);
    statement = Mockito.mock(Statement.class);
    connection = Mockito.mock(Connection.class);
    preparedStatement = Mockito.mock(PreparedStatement.class);
    attributeResultSet = Mockito.mock(ResultSet.class);
    defaultSchemaReader = new OracleSourceSchemaReader();

    Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
    Mockito.when(resultSet.getStatement()).thenReturn(statement);
    Mockito.when(statement.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(attributeResultSet);
  }

  private void mockSingleColumn(int sqlType, String columnName, String columnTypeName) throws SQLException {
    Mockito.when(metadata.getColumnCount()).thenReturn(1);
    Mockito.when(metadata.getColumnType(1)).thenReturn(sqlType);
    Mockito.when(metadata.getColumnName(1)).thenReturn(columnName);
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn(columnTypeName);
  }

  private ResultSet mockAttribute(String name, String typeName, String owner) throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    Mockito.when(rs.next()).thenReturn(true, false);
    Mockito.when(rs.getString("ATTR_NAME")).thenReturn(name);
    Mockito.when(rs.getString("ATTR_TYPE_NAME")).thenReturn(typeName);
    Mockito.when(rs.getString("ATTR_TYPE_OWNER")).thenReturn(owner);
    return rs;
  }

  private ResultSet mockAttribute(String name, String typeName, int precision, int scale) throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    Mockito.when(rs.next()).thenReturn(true, false);
    Mockito.when(rs.getString("ATTR_NAME")).thenReturn(name);
    Mockito.when(rs.getString("ATTR_TYPE_NAME")).thenReturn(typeName);
    Mockito.when(rs.getInt("PRECISION")).thenReturn(precision);
    Mockito.when(rs.getInt("SCALE")).thenReturn(scale);
    return rs;
  }

  private void mockStatements(ResultSet... resultSets) throws SQLException {
    PreparedStatement[] statements = new PreparedStatement[resultSets.length];
    for (int i = 0; i < resultSets.length; i++) {
      statements[i] = Mockito.mock(PreparedStatement.class);
      Mockito.when(statements[i].executeQuery()).thenReturn(resultSets[i]);
    }
    if (statements.length == 1) {
      Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statements[0]);
    } else if (statements.length > 1) {
      Mockito.when(connection.prepareStatement(Mockito.anyString()))
        .thenReturn(statements[0], Arrays.copyOfRange(statements, 1, statements.length));
    }
  }

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
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader(null, false,
            false, false, false);
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
  public void getSchemaFields_structType_returnsRecord() throws SQLException {
    mockSingleColumn(Types.STRUCT, "address", "CS_ITN.ADDRESS_TYPE");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST_SCHEMA");
    Boolean[] nextReturns = new Boolean[27];
    Arrays.fill(nextReturns, 0, 26, true);
    nextReturns[26] = false;
    Mockito.when(attributeResultSet.next()).thenReturn(true, nextReturns);
    Mockito.when(attributeResultSet.getString("ATTR_NAME")).thenReturn(
      "ATTR_VARCHAR2", "ATTR_VARCHAR", "ATTR_CHAR", "ATTR_CHAR2", "ATTR_NCHAR",
      "ATTR_NVARCHAR2", "ATTR_CLOB", "ATTR_NCLOB",
      "ATTR_UROWID", "ATTR_NUMBER_PREC", "ATTR_NUMBER_NOPREC", "ATTR_DECIMAL",
      "ATTR_INTEGER", "ATTR_FLOAT", "ATTR_REAL", "ATTR_DOUBLE", "ATTR_BINARY_FLOAT",
      "ATTR_BINARY_DOUBLE", "ATTR_DATE", "ATTR_TIMESTAMP", "ATTR_TIMESTAMP_TZ",
      "ATTR_TIMESTAMP_LTZ", "ATTR_INTERVAL_DS", "ATTR_INTERVAL_YM", "ATTR_BLOB",
      "ATTR_RAW", "ATTR_BFILE"
    );
    Mockito.when(attributeResultSet.getString("ATTR_TYPE_NAME")).thenReturn(
      "VARCHAR2", "VARCHAR", "CHAR", "CHAR2", "NCHAR",
      "NVARCHAR2", "CLOB", "NCLOB",
      "UROWID", "NUMBER", "NUMBER", "DECIMAL",
      "INTEGER", "FLOAT", "REAL", "DOUBLE", "BINARY_FLOAT",
      "BINARY_DOUBLE", "DATE", "TIMESTAMP", "TIMESTAMP WITH TZ",
      "TIMESTAMP WITH LOCAL TZ", "INTERVAL DAY TO SECOND", "INTERVAL YEAR TO MONTH", "BLOB",
      "RAW", "BFILE"
    );
    Mockito.when(attributeResultSet.getInt("PRECISION")).thenReturn(
      50, 50, 10, 10, 10,
      50, 0, 0,
      0, 10, 0, 8,
      10, 10, 10, 10, 0,
      0, 0, 0, 0,
      0, 0, 0, 0,
      100, 0
    );
    Mockito.when(attributeResultSet.getInt("SCALE")).thenReturn(
      0, 0, 0, 0, 0,
      0, 0, 0,
      0, 2, 0, 2,
      0, 0, 0, 0, 0,
      0, 0, 0, 0,
      0, 0
    );
    Schema expectedAddressSchema = Schema.recordOf("address",
      Schema.Field.of("ATTR_VARCHAR2", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_VARCHAR", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_CHAR", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_CHAR2", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_NCHAR", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_NVARCHAR2", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_CLOB", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_NCLOB", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_UROWID", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_NUMBER_PREC", Schema.nullableOf(Schema.decimalOf(10, 2))),
      Schema.Field.of("ATTR_NUMBER_NOPREC", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_DECIMAL", Schema.nullableOf(Schema.decimalOf(8, 2))),
      Schema.Field.of("ATTR_INTEGER", Schema.nullableOf(Schema.decimalOf(10, 0))),
      Schema.Field.of("ATTR_FLOAT", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("ATTR_REAL", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("ATTR_DOUBLE", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("ATTR_BINARY_FLOAT", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("ATTR_BINARY_DOUBLE", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("ATTR_DATE", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))),
      Schema.Field.of("ATTR_TIMESTAMP", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))),
      Schema.Field.of("ATTR_TIMESTAMP_TZ", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("ATTR_TIMESTAMP_LTZ", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))),
      Schema.Field.of("ATTR_INTERVAL_DS", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_INTERVAL_YM", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ATTR_BLOB", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("ATTR_RAW", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("ATTR_BFILE", Schema.nullableOf(Schema.of(Schema.Type.BYTES)))
    );
    Schema expectedSchema = Schema.recordOf("record",
      Schema.Field.of("address", expectedAddressSchema)
    );

    Assert.assertEquals(expectedSchema, Schema.recordOf("record",
            defaultSchemaReader.getSchemaFields(resultSet)));
  }

  @Test
  public void getSchema_xmlField_returnString() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader(null,
            false, false, false, true);
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
  public void getSchemaFields_structUnsupportedType_throwsException() throws SQLException {
    mockSingleColumn(Types.STRUCT, "complex_payload", "CS_ITN.ANYDATA_TYPE");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST_SCHEMA");
    Mockito.when(attributeResultSet.next()).thenReturn(true, true, false);
    Mockito.when(attributeResultSet.getString("ATTR_NAME"))
            .thenReturn("VALID_ID", "UNSUPPORTED_DATA");
    Mockito.when(attributeResultSet.getString("ATTR_TYPE_NAME"))
            .thenReturn("NUMBER", "ANYDATA");
    Mockito.when(attributeResultSet.getInt("PRECISION")).thenReturn(10, 0);
    Mockito.when(attributeResultSet.getInt("SCALE")).thenReturn(0, 0);

    Assert.assertThrows(ProgramFailureException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_nestedStructLevel_returnsRecord() throws SQLException {
    mockStatements(
      mockAttribute("SUB1", "STRUCT_L1", "TEST"),
      mockAttribute("SUB2", "STRUCT_L2", "TEST"),
      mockAttribute("SUB3", "STRUCT_L3", "TEST"),
      mockAttribute("ID", "VARCHAR2", 50, 0)
    );
    mockSingleColumn(Types.STRUCT, "payload", "TEST.STRUCT_L0");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST");

    Schema level3Schema = Schema.recordOf("SUB3",
      Schema.Field.of("ID", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema level2Schema = Schema.recordOf("SUB2",
      Schema.Field.of("SUB3", Schema.nullableOf(level3Schema)));
    Schema level1Schema = Schema.recordOf("SUB1",
      Schema.Field.of("SUB2", Schema.nullableOf(level2Schema)));
    Schema level0Schema = Schema.recordOf("payload",
      Schema.Field.of("SUB1", Schema.nullableOf(level1Schema)));
    Schema expectedSchema = Schema.recordOf("record",
      Schema.Field.of("payload", level0Schema));

    Assert.assertEquals(expectedSchema, Schema.recordOf("record",
            defaultSchemaReader.getSchemaFields(resultSet)));
  }

  @Test
  public void getSchemaFields_exceedsNestedStructLevel_throwsException() throws SQLException {
    mockStatements(
      mockAttribute("SUB1", "STRUCT_L1", "TEST"),
      mockAttribute("SUB2", "STRUCT_L2", "TEST"),
      mockAttribute("SUB3", "STRUCT_L3", "TEST"),
      mockAttribute("SUB4", "STRUCT_L4", "TEST")
    );
    mockSingleColumn(Types.STRUCT, "payload", "TEST.STRUCT_L0");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST");

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_multipleStructColumns_returnsRecord() throws SQLException {
    ResultSet attrRs1 = mockAttribute("STREET", "VARCHAR2", 50, 0);
    ResultSet attrRs2 = mockAttribute("STREET", "VARCHAR2", 50, 0);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(attrRs1, attrRs2);
    Mockito.when(metadata.getColumnCount()).thenReturn(2);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.STRUCT);
    Mockito.when(metadata.getColumnName(1)).thenReturn("address1");
    Mockito.when(metadata.getColumnTypeName(1)).thenReturn("CS_ITN.ADDRESS_TYPE");
    Mockito.when(metadata.getSchemaName(1)).thenReturn("TEST_SCHEMA");
    Mockito.when(metadata.getColumnType(2)).thenReturn(Types.STRUCT);
    Mockito.when(metadata.getColumnName(2)).thenReturn("address2");
    Mockito.when(metadata.getColumnTypeName(2)).thenReturn("CS_ITN.ADDRESS_TYPE");
    Mockito.when(metadata.getSchemaName(2)).thenReturn("TEST_SCHEMA");
    Schema address1Schema = Schema.recordOf("address1",
      Schema.Field.of("STREET", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema address2Schema = Schema.recordOf("address2",
      Schema.Field.of("STREET", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema expectedSchema = Schema.recordOf("record",
      Schema.Field.of("address1", address1Schema),
      Schema.Field.of("address2", address2Schema)
    );

    Assert.assertEquals(expectedSchema, Schema.recordOf("record",
            defaultSchemaReader.getSchemaFields(resultSet)));
  }

  @Test
  public void getSchemaFields_unqualifiedStructType_returnsRecord() throws SQLException {
    PreparedStatement ownerStmt = Mockito.mock(PreparedStatement.class);
    ResultSet ownerRs = Mockito.mock(ResultSet.class);
    ResultSet attrRs = mockAttribute("STREET", "VARCHAR2", 50, 0);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(ownerStmt, preparedStatement);
    Mockito.when(ownerStmt.executeQuery()).thenReturn(ownerRs);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(attrRs);
    mockSingleColumn(Types.STRUCT, "address", "ADDRESS_TYPE");
    Mockito.when(ownerRs.next()).thenReturn(true);
    Mockito.when(ownerRs.getString("DATA_TYPE_OWNER")).thenReturn("CS_ITN");

    Schema expectedAddressSchema = Schema.recordOf("address",
      Schema.Field.of("STREET", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema expectedSchema = Schema.recordOf("record",
      Schema.Field.of("address", expectedAddressSchema)
    );

    Assert.assertEquals(expectedSchema, Schema.recordOf("record",
            defaultSchemaReader.getSchemaFields(resultSet)));
  }

  @Test
  public void getSchemaFields_unqualifiedStructOwnerNotFound_throwsException() throws SQLException {
    PreparedStatement ownerStmt = Mockito.mock(PreparedStatement.class);
    ResultSet ownerRs = Mockito.mock(ResultSet.class);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(ownerStmt, preparedStatement);
    Mockito.when(ownerStmt.executeQuery()).thenReturn(ownerRs);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(attributeResultSet);
    mockSingleColumn(Types.STRUCT, "address", "ADDRESS_TYPE");
    Mockito.when(ownerRs.next()).thenReturn(false);
    Mockito.when(attributeResultSet.next()).thenReturn(false);

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_unqualifiedStructOwnerQueryTimeout_throwsException() throws SQLException {
    mockSingleColumn(Types.STRUCT, "address", "ADDRESS_TYPE");
    Mockito.when(connection.prepareStatement(Mockito.anyString()))
      .thenThrow(new SQLTimeoutException("Query timed out"));

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_structWithNoAttributes_throwsException() throws SQLException {
    mockSingleColumn(Types.STRUCT, "address", "CS_ITN.ADDRESS_TYPE");
    Mockito.when(attributeResultSet.next()).thenReturn(false);

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_nestedStructEmptyTypeOwner_throwsException() throws SQLException {
    mockSingleColumn(Types.STRUCT, "address", "CS_ITN.ADDRESS_TYPE");
    Mockito.when(attributeResultSet.next()).thenReturn(true, false);
    Mockito.when(attributeResultSet.getString("ATTR_NAME")).thenReturn("SUB_STRUCT");
    Mockito.when(attributeResultSet.getString("ATTR_TYPE_NAME")).thenReturn("NESTED_TYPE");
    Mockito.when(attributeResultSet.getString("ATTR_TYPE_OWNER")).thenReturn("");

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_nestedStructNullTypeOwner_throwsException() throws SQLException {
    mockSingleColumn(Types.STRUCT, "address", "CS_ITN.ADDRESS_TYPE");
    Mockito.when(attributeResultSet.next()).thenReturn(true, false);
    Mockito.when(attributeResultSet.getString("ATTR_NAME")).thenReturn("SUB_STRUCT");
    Mockito.when(attributeResultSet.getString("ATTR_TYPE_NAME")).thenReturn("NESTED_TYPE");
    Mockito.when(attributeResultSet.getString("ATTR_TYPE_OWNER")).thenReturn(null);

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchema_precisionlessNumericAndOldTimestampBehavior_returnsExpectedSchema() throws SQLException {
    OracleSourceSchemaReader schemaReader = new OracleSourceSchemaReader("sess1", true,
            true, false, false);
    Mockito.when(metadata.getColumnCount()).thenReturn(9);
    Mockito.when(metadata.getColumnType(1)).thenReturn(OracleSourceSchemaReader.TIMESTAMP_TZ);
    Mockito.when(metadata.getColumnName(1)).thenReturn("tzCol");
    Mockito.when(metadata.getColumnType(2)).thenReturn(Types.NUMERIC);
    Mockito.when(metadata.getColumnName(2)).thenReturn("doubleNumCol");
    Mockito.when(metadata.getColumnClassName(2)).thenReturn(Double.class.getTypeName());
    Mockito.when(metadata.getColumnType(3)).thenReturn(Types.NUMERIC);
    Mockito.when(metadata.getColumnName(3)).thenReturn("precisionlessDecCol");
    Mockito.when(metadata.getPrecision(3)).thenReturn(0);
    Mockito.when(metadata.getColumnType(4)).thenReturn(Types.VARCHAR);
    Mockito.when(metadata.getColumnName(4)).thenReturn("varcharCol");
    Mockito.when(metadata.getColumnType(5)).thenReturn(Types.TIMESTAMP);
    Mockito.when(metadata.getColumnName(5)).thenReturn("oldTsCol");
    Mockito.when(metadata.getColumnType(6)).thenReturn(OracleSourceSchemaReader.TIMESTAMP_LTZ);
    Mockito.when(metadata.getColumnName(6)).thenReturn("oldLtzCol");
    Mockito.when(metadata.getColumnType(7)).thenReturn(OracleSourceSchemaReader.LONG);
    Mockito.when(metadata.getColumnName(7)).thenReturn("longCol");
    Mockito.when(metadata.getColumnType(8)).thenReturn(Types.VARCHAR);
    Mockito.when(metadata.getColumnName(8)).thenReturn("c_sess1");
    Mockito.when(metadata.getColumnType(9)).thenReturn(Types.VARCHAR);
    Mockito.when(metadata.getColumnName(9)).thenReturn("s_sess1");

    List<Schema.Field> actualFields = schemaReader.getSchemaFields(resultSet);

    Assert.assertEquals(7, actualFields.size());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), actualFields.get(0).getSchema());
    Assert.assertEquals(Schema.of(Schema.Type.DOUBLE), actualFields.get(1).getSchema());
    Assert.assertEquals(Schema.decimalOf(38, 0), actualFields.get(2).getSchema());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), actualFields.get(3).getSchema());
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS), actualFields.get(4).getSchema());
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS), actualFields.get(5).getSchema());
    Assert.assertEquals(Schema.of(Schema.Type.STRING), actualFields.get(6).getSchema());
  }

  @Test
  public void getSchemaFields_nullStatement_throwsNullPointerException() throws SQLException {
    Mockito.when(resultSet.getStatement()).thenReturn(null);

    Assert.assertThrows(NullPointerException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_nullConnectionForStruct_throwsNullPointerException() throws SQLException {
    Mockito.when(statement.getConnection()).thenReturn(null);
    mockSingleColumn(Types.STRUCT, "address", "CS_ITN.ADDRESS_TYPE");

    Assert.assertThrows(NullPointerException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_structNullColumnTypeName_throwsNullPointerException() throws SQLException {
    PreparedStatement ownerStmt = Mockito.mock(PreparedStatement.class);
    ResultSet ownerRs = Mockito.mock(ResultSet.class);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(ownerStmt, preparedStatement);
    Mockito.when(ownerStmt.executeQuery()).thenReturn(ownerRs);
    Mockito.when(ownerRs.next()).thenReturn(true);
    Mockito.when(ownerRs.getString("DATA_TYPE_OWNER")).thenReturn("CS_ITN");
    mockSingleColumn(Types.STRUCT, "address", null);

    Assert.assertThrows(NullPointerException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_numericNegativePrecision_throwsIllegalArgumentException() throws SQLException {
    mockSingleColumn(Types.NUMERIC, "negPrecCol", "NUMBER");
    Mockito.when(metadata.getPrecision(1)).thenReturn(-1);
    Mockito.when(metadata.getScale(1)).thenReturn(0);

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_numericMinIntegerPrecision_throwsIllegalArgumentException() throws SQLException {
    mockSingleColumn(Types.NUMERIC, "minPrecCol", "NUMBER");
    Mockito.when(metadata.getPrecision(1)).thenReturn(Integer.MIN_VALUE);
    Mockito.when(metadata.getScale(1)).thenReturn(0);

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_structAttributeNegativePrecision_throwsIllegalArgumentException() throws SQLException {
    mockSingleColumn(Types.STRUCT, "address", "CS_ITN.ADDRESS_TYPE");
    Mockito.when(attributeResultSet.next()).thenReturn(true, false);
    Mockito.when(attributeResultSet.getString("ATTR_NAME")).thenReturn("BAD_NUM");
    Mockito.when(attributeResultSet.getString("ATTR_TYPE_NAME")).thenReturn("NUMBER");
    Mockito.when(attributeResultSet.getInt("PRECISION")).thenReturn(-5);
    Mockito.when(attributeResultSet.getInt("SCALE")).thenReturn(10);

    Assert.assertThrows(IllegalArgumentException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }

  @Test
  public void getSchemaFields_structAttributeQueryTimeout_throwsSQLTimeoutException() throws SQLException {
    mockSingleColumn(Types.STRUCT, "address", "CS_ITN.ADDRESS_TYPE");
    Mockito.when(preparedStatement.executeQuery())
      .thenThrow(new SQLTimeoutException("Timeout fetching struct attributes"));

    Assert.assertThrows(SQLTimeoutException.class, () -> defaultSchemaReader.getSchemaFields(resultSet));
  }
}

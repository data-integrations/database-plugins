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
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.ConnectionConfigAccessor;
import oracle.jdbc.OracleBfile;
import oracle.sql.TIMESTAMPTZ;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.PipedInputStream;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;

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

  @Mock
  Statement statement;

  @Mock
  Connection connection;

  @Mock
  PreparedStatement preparedStatement;

  private OracleSourceDBRecord dbRecord;

  /**
   * Mock interface for Oracle Struct containing getDescriptor method.
   */
  public interface MockOracleStruct extends Struct {
    Object getDescriptor() throws Exception;
  }

  /**
   * Mock interface for Oracle StructDescriptor containing getMetaData method.
   */
  public interface MockStructDescriptor {
    ResultSetMetaData getMetaData() throws Exception;
  }

  @Before
  public void setUp() throws SQLException {
    dbRecord = new OracleSourceDBRecord(null, null);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getStatement()).thenReturn(statement);
    when(statement.getConnection()).thenReturn(connection);
  }

  private MockOracleStruct createMockStruct(String[] columnNames, Object[] attributes) throws Exception {
    MockOracleStruct structMock = Mockito.mock(MockOracleStruct.class);
    MockStructDescriptor descriptorMock = Mockito.mock(MockStructDescriptor.class);
    ResultSetMetaData structMetaData = Mockito.mock(ResultSetMetaData.class);
    when(structMock.getAttributes()).thenReturn(attributes);
    when(structMock.getDescriptor()).thenReturn(descriptorMock);
    when(descriptorMock.getMetaData()).thenReturn(structMetaData);
    when(structMetaData.getColumnCount()).thenReturn(columnNames.length);
    for (int i = 0; i < columnNames.length; i++) {
      when(structMetaData.getColumnName(eq(i + 1))).thenReturn(columnNames[i]);
    }
    return structMock;
  }

  /**
   * Validate the precision less Numbers handling against following use cases.
   * 1. Ensure that for Number(0,-127) non nullable type a String type is returned if output schema is String.
   * 2. Ensure that for Number(0,-127) non nullable type a String type is returned if output schema is String.
   * 3. Ensure that for Number(0,-127) nullable type a String type is returned if output schema is String.
   * 4. Ensure that for Number(0,-127) nullable type a String type is returned if output schema is String.
   * @throws Exception
   */
  @Test
  public void validatePrecisionLessNumberParsingForOutputSchemaAsString() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.of(Schema.Type.STRING));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.of(Schema.Type.STRING));
    Schema.Field field3 = Schema.Field.of("ID3", Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema.Field field4 = Schema.Field.of("ID4", Schema.nullableOf(Schema.of(Schema.Type.STRING)));

    Schema schema = Schema.recordOf(
        "dbRecord",
        field1,
        field2,
        field3,
        field4
    );

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getString(eq(1))).thenReturn("123");
    when(resultSet.getString(eq(2))).thenReturn("123.4568");
    when(resultSet.getString(eq(3))).thenReturn("123");
    when(resultSet.getString(eq(4))).thenReturn("123.4568");

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field2, 2, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field3, 3, Types.NUMERIC, 0, -127);
    dbRecord.handleField(resultSet, builder, field4, 4, Types.NUMERIC, 0, -127);

    StructuredRecord record = builder.build();
    Assert.assertTrue(record.get("ID1") instanceof String);
    Assert.assertEquals(record.get("ID1"), "123");
    Assert.assertTrue(record.get("ID2") instanceof String);
    Assert.assertEquals(record.get("ID2"), "123.4568");
    Assert.assertTrue(record.get("ID3") instanceof String);
    Assert.assertEquals(record.get("ID3"), "123");
    Assert.assertTrue(record.get("ID4") instanceof String);
    Assert.assertEquals(record.get("ID4"), "123.4568");
  }

  /**
   * Validate the precision less Numbers handling against following use cases.
   * 1. Ensure that for Number(0,-127) non nullable type a Decimal type is returned if output schema is Decimal.
   * 2. Ensure that for Number(0,-127) non nullable type a String is returned if output schema is Decimal.
   * 3. Ensure that for Number(0,-127) nullable type a String type is returned if output schema is Decimal.
   * 4. Ensure that for Number(0,-127) nullable type a String is returned if output schema is Decimal.
   * @throws Exception
   */
  @Test
  public void validatePrecisionLessNumberParsingForOutputSchemaAsDecimal() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.decimalOf(DEFAULT_PRECISION));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.decimalOf(DEFAULT_PRECISION, 4));
    Schema.Field field3 = Schema.Field.of("ID3", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION)));
    Schema.Field field4 = Schema.Field.of("ID4", Schema.decimalOf(DEFAULT_PRECISION, 4));

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
    Assert.assertTrue(record.getDecimal("ID1") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID1").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID2") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID2").toPlainString(), "123.4568");
    Assert.assertTrue(record.getDecimal("ID3") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID3").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID4") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID4").toPlainString(), "123.4568");
  }

  /**
   * Validate the default precision Numbers handling against following use cases.
   * 1. Ensure that for Number(38, 0) non nullable type a Number(38,0) is returned if output schema is Decimal.
   * 2. Ensure that for Number(38, 4) non nullable type a Number(38,6) is returned if output schema is Decimal.
   * 3. Ensure that for Number(38, 0) nullable type a Number(38,0) is returned if output schema is Decimal.
   * 4. Ensure that for Number(38, 4) nullable type a Number(38,6) is returned if output schema is Decimal.
   * @throws Exception
   */
  @Test
  public void validateDefaultDecimalParsing() throws Exception {
    Schema.Field field1 = Schema.Field.of("ID1", Schema.decimalOf(DEFAULT_PRECISION));
    Schema.Field field2 = Schema.Field.of("ID2", Schema.decimalOf(DEFAULT_PRECISION, 6));
    Schema.Field field3 = Schema.Field.of("ID3", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION)));
    Schema.Field field4 = Schema.Field.of("ID4", Schema.nullableOf(Schema.decimalOf(DEFAULT_PRECISION, 6)));

    Schema schema = Schema.recordOf(
        "dbRecord",
        field1,
        field2,
        field3,
        field4
    );

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getBigDecimal(eq(1), eq(0))).thenReturn(new BigDecimal("123"));
    when(resultSet.getBigDecimal(eq(2), eq(6))).thenReturn(new BigDecimal("123.456789"));
    when(resultSet.getBigDecimal(eq(3), eq(0))).thenReturn(new BigDecimal("123"));
    when(resultSet.getBigDecimal(eq(4), eq(6))).thenReturn(new BigDecimal("123.456789"));

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, Types.NUMERIC, DEFAULT_PRECISION, 0);
    dbRecord.handleField(resultSet, builder, field2, 2, Types.NUMERIC, DEFAULT_PRECISION, 4);
    dbRecord.handleField(resultSet, builder, field3, 3, Types.NUMERIC, DEFAULT_PRECISION, 0);
    dbRecord.handleField(resultSet, builder, field4, 4, Types.NUMERIC, DEFAULT_PRECISION, 4);

    StructuredRecord record = builder.build();
    Assert.assertTrue(record.getDecimal("ID1") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID1").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID2") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID2").toPlainString(), "123.456789");
    Assert.assertTrue(record.getDecimal("ID3") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID3").toPlainString(), "123");
    Assert.assertTrue(record.getDecimal("ID4") instanceof BigDecimal);
    Assert.assertEquals(record.getDecimal("ID4").toPlainString(), "123.456789");
  }

  /**
   * Validate the Null value for TimestampLTZ datatype.
   * @throws Exception
   */
  @Test
  public void validateTimestampLTZTypeNullHandling() throws Exception {
    Schema.Field field1 = Schema.Field.of("field1", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    Schema schema = Schema.recordOf(
            "dbRecord",
            field1
    );

    when(resultSet.getTimestamp(eq(1))).thenReturn(null);

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, OracleSourceSchemaReader.TIMESTAMP_LTZ, DEFAULT_PRECISION, 0);

    StructuredRecord record = builder.build();
    Assert.assertNull(record.getTimestamp("field1"));
  }

  /***
   * Validate the TimestampTZ type handling in the OracleSourceDBRecord code
   * @throws Exception
   */
  @Test
  public void validateTimestampTZTypeNullHandling() throws Exception {
    Schema.Field field1 = Schema.Field.of("field1", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    Schema schema = Schema.recordOf(
            "dbRecord",
            field1
    );

    when(resultSet.getObject(eq(1))).thenReturn(null);

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(null, null);
    dbRecord.handleField(resultSet, builder, field1, 1, OracleSourceSchemaReader.TIMESTAMP_TZ, DEFAULT_PRECISION, 0);

    StructuredRecord record = builder.build();
    Assert.assertNull(record.get("field1"));
  }

  @Test
  public void populateStructField_nullValue_setsFieldToNull() throws Exception {
    Schema addressSchema = Schema.recordOf("ADDRESS_TYPE",
      Schema.Field.of("STREET", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("CITY", Schema.of(Schema.Type.STRING)));
    Schema.Field addressField = Schema.Field.of("address", Schema.nullableOf(addressSchema));
    Schema schema = Schema.recordOf("dbRecord", addressField);
    when(resultSet.getObject(eq(1))).thenReturn(null);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    dbRecord.handleField(resultSet, builder, addressField, 1, Types.STRUCT,
            DEFAULT_PRECISION, 0);

    StructuredRecord record = builder.build();
    Assert.assertNull(record.get("address"));
  }

  @Test
  public void handleStructField_nonNullValue_setsFieldCorrectly() throws Exception {
    Schema addressSchema = Schema.recordOf("ADDRESS_TYPE",
      Schema.Field.of("STREET", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("CITY", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ZIPCODE", Schema.of(Schema.Type.INT)));
    Schema.Field addressField = Schema.Field.of("address", addressSchema);
    Schema schema = Schema.recordOf("dbRecord", addressField);
    MockOracleStruct structMock = createMockStruct(new String[]{ "STREET", "CITY", "ZIPCODE" },
      new Object[]{ "123 Main St", "San Francisco", 94105 });
    when(resultSet.getObject(eq(1))).thenReturn(structMock);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    dbRecord.handleField(resultSet, builder, addressField, 1, Types.STRUCT,
            DEFAULT_PRECISION, 0);

    StructuredRecord record = builder.build();
    StructuredRecord addressRecord = record.get("address");
    Assert.assertNotNull(addressRecord);
    Assert.assertEquals("123 Main St", addressRecord.get("STREET"));
    Assert.assertEquals("San Francisco", addressRecord.get("CITY"));
    Assert.assertEquals(Integer.valueOf(94105), addressRecord.get("ZIPCODE"));
  }

  @Test
  public void handleStructField_nestedStructure_setsFieldCorrectly() throws Exception {
    Schema locationSchema = Schema.recordOf("LOCATION_TYPE",
      Schema.Field.of("LATITUDE", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("LONGITUDE", Schema.of(Schema.Type.DOUBLE)));
    Schema addressSchema = Schema.recordOf("ADDRESS_TYPE",
      Schema.Field.of("STREET", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("LOCATION", locationSchema));
    Schema.Field addressField = Schema.Field.of("address", addressSchema);
    Schema schema = Schema.recordOf("dbRecord", addressField);
    MockOracleStruct locationStructMock = createMockStruct(new String[]{ "LATITUDE", "LONGITUDE" },
      new Object[]{ Double.valueOf(37.7749), Double.valueOf(-122.4194) });
    MockOracleStruct addressStructMock = createMockStruct(new String[]{ "STREET", "LOCATION" },
      new Object[]{ "123 Main St", locationStructMock });
    when(resultSet.getObject(eq(1))).thenReturn(addressStructMock);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    dbRecord.handleField(resultSet, builder, addressField, 1, Types.STRUCT,
            DEFAULT_PRECISION, 0);

    StructuredRecord record = builder.build();
    StructuredRecord addressRecord = record.get("address");
    Assert.assertNotNull(addressRecord);
    Assert.assertEquals("123 Main St", addressRecord.get("STREET"));
    StructuredRecord locationRecord = addressRecord.get("LOCATION");
    Assert.assertNotNull(locationRecord);
    Assert.assertEquals(Double.valueOf(37.7749), locationRecord.get("LATITUDE"));
    Assert.assertEquals(Double.valueOf(-122.4194), locationRecord.get("LONGITUDE"));
  }

  @Test
  public void getAttributeMap_nullAttributes_returnsEmptyMap() throws Exception {
    Schema addressSchema = Schema.recordOf("ADDRESS_TYPE",
      Schema.Field.of("STREET", Schema.of(Schema.Type.STRING)));
    MockOracleStruct structMock = Mockito.mock(MockOracleStruct.class);
    when(structMock.getAttributes()).thenReturn(null);

    Map<String, Object> attributeMap = dbRecord.getAttributeMap(structMock, addressSchema, connection);

    Assert.assertTrue(attributeMap.isEmpty());
  }

  @Test
  public void getAttributeMap_fewerAttributesThanColumns_mapsAvailableAttributes() throws Exception {
    Schema addressSchema = Schema.recordOf("ADDRESS_TYPE",
      Schema.Field.of("STREET", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("CITY", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    MockOracleStruct structMock = createMockStruct(new String[]{ "STREET", "CITY" },
      new Object[]{ "123 Main St" });

    Map<String, Object> attributeMap = dbRecord.getAttributeMap(structMock, addressSchema, connection);

    Assert.assertEquals(1, attributeMap.size());
    Assert.assertEquals("123 Main St", attributeMap.get("STREET"));
  }

  @Test
  public void getAttributeMap_nullDescriptor_throwsSQLException() throws Exception {
    Schema addressSchema = Schema.recordOf("ADDRESS_TYPE",
      Schema.Field.of("STREET", Schema.of(Schema.Type.STRING)));
    MockOracleStruct structMock = Mockito.mock(MockOracleStruct.class);
    when(structMock.getAttributes()).thenReturn(new Object[]{ "123 Main St" });
    when(structMock.getDescriptor()).thenReturn(null);

    Assert.assertThrows(SQLException.class, () -> dbRecord.getAttributeMap(structMock, addressSchema, connection));
  }

  @Test
  public void getBfileBytes_nullOrNonExistentFile_returnsNull() throws Exception {
    OracleBfile bfileMock = Mockito.mock(OracleBfile.class);
    when(bfileMock.fileExists()).thenReturn(false);

    Assert.assertNull(dbRecord.getBfileBytes((Object) null, "BFILE_COL"));
    Assert.assertNull(dbRecord.getBfileBytes(bfileMock, "BFILE_COL"));
  }

  @Test
  public void getBfileBytes_uninitializedBfileOrUnconnectedStream_throwsInvalidStageException() throws Exception {
    Object uninitializedBfileProxy = Proxy.newProxyInstance(OracleBfile.class.getClassLoader(),
      new Class<?>[]{ OracleBfile.class }, (proxy, method, args) -> null);
    OracleBfile bfileWithUnconnectedPipe = Mockito.mock(OracleBfile.class);
    when(bfileWithUnconnectedPipe.fileExists()).thenReturn(true);
    when(bfileWithUnconnectedPipe.getBinaryStream()).thenReturn(new PipedInputStream());

    Assert.assertThrows(InvalidStageException.class,
                        () -> dbRecord.getBfileBytes(uninitializedBfileProxy, "BFILE_COL"));
    Assert.assertThrows(InvalidStageException.class,
                        () -> dbRecord.getBfileBytes(bfileWithUnconnectedPipe, "BFILE_COL"));
  }

  @Test
  public void handleField_bfileColumnWithExistingFile_setsBytesCorrectly() throws Exception {
    Schema.Field bfileField = Schema.Field.of("BFILE_COL", Schema.of(Schema.Type.BYTES));
    Schema schema = Schema.recordOf("dbRecord", bfileField);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    OracleBfile bfileMock = Mockito.mock(OracleBfile.class);
    byte[] expectedBytes = new byte[]{ 10, 20, 30 };
    when(resultSetMetaData.getColumnName(eq(1))).thenReturn("BFILE_COL");
    when(resultSet.getObject(eq("BFILE_COL"))).thenReturn(bfileMock);
    when(bfileMock.fileExists()).thenReturn(true);
    when(bfileMock.getBinaryStream()).thenReturn(new ByteArrayInputStream(expectedBytes));

    dbRecord.handleField(resultSet, builder, bfileField, 1, OracleSourceSchemaReader.BFILE,
            0, 0);

    StructuredRecord record = builder.build();
    Assert.assertArrayEquals(expectedBytes, record.get("BFILE_COL"));
  }

  @Test
  public void handleField_numericAndBinaryTypes_setsFieldsCorrectly() throws Exception {
    Schema.Field nclobField = Schema.Field.of("NCLOB_COL", Schema.of(Schema.Type.STRING));
    Schema.Field bfloatField = Schema.Field.of("BFLOAT_COL", Schema.of(Schema.Type.FLOAT));
    Schema.Field bdoubleField = Schema.Field.of("BDOUBLE_COL", Schema.of(Schema.Type.DOUBLE));
    Schema.Field numDoubleField = Schema.Field.of("NUM_DOUBLE_COL", Schema.of(Schema.Type.DOUBLE));
    Schema schema = Schema.recordOf("dbRecord", nclobField, bfloatField, bdoubleField, numDoubleField);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    when(resultSet.getString(eq(1))).thenReturn("nclobVal");
    when(resultSet.getFloat(eq(2))).thenReturn(1.5f);
    when(resultSet.getDouble(eq(3))).thenReturn(2.5d);
    when(resultSetMetaData.getColumnClassName(eq(4))).thenReturn(Double.class.getTypeName());
    when(resultSet.getDouble(eq(4))).thenReturn(3.5d);

    dbRecord.handleField(resultSet, builder, nclobField, 1, Types.NCLOB, 0, 0);
    dbRecord.handleField(resultSet, builder, bfloatField, 2, OracleSourceSchemaReader.BINARY_FLOAT,
            0, 0);
    dbRecord.handleField(resultSet, builder, bdoubleField, 3, OracleSourceSchemaReader.BINARY_DOUBLE,
            0, 0);
    dbRecord.handleField(resultSet, builder, numDoubleField, 4, Types.NUMERIC, 10, 2);

    StructuredRecord record = builder.build();
    Assert.assertEquals("nclobVal", record.get("NCLOB_COL"));
    Assert.assertEquals(1.5f, record.get("BFLOAT_COL"), 0.001f);
    Assert.assertEquals(2.5d, record.get("BDOUBLE_COL"), 0.001d);
    Assert.assertEquals(3.5d, record.get("NUM_DOUBLE_COL"), 0.001d);
  }

  @Test
  public void handleField_temporalTypes_setsFieldsCorrectly() throws Exception {
    Schema.Field tzStringField = Schema.Field.of("TZ_STR_COL", Schema.of(Schema.Type.STRING));
    Schema.Field tzMicrosField = Schema.Field.of("TZ_MICROS_COL",
            Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    Schema.Field tsDateTimeField = Schema.Field.of("TS_DT_COL", Schema.of(Schema.LogicalType.DATETIME));
    Schema.Field tsNullDateTimeField = Schema.Field.of("TS_NULL_DT_COL",
            Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME)));
    Schema.Field tsMicrosField = Schema.Field.of("TS_MICROS_COL",
            Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    Schema.Field ltzDateTimeField = Schema.Field.of("LTZ_DT_COL", Schema.of(Schema.LogicalType.DATETIME));
    Schema.Field ltzNullDateTimeField = Schema.Field.of("LTZ_NULL_DT_COL",
            Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME)));
    Schema.Field ltzMicrosField = Schema.Field.of("LTZ_MICROS_COL",
            Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    Schema schema = Schema.recordOf("dbRecord", tzStringField, tzMicrosField, tsDateTimeField,
      tsNullDateTimeField, tsMicrosField, ltzDateTimeField, ltzNullDateTimeField, ltzMicrosField);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Timestamp nowTs = Timestamp.valueOf("2024-05-15 10:30:00");
    OffsetDateTime expectedOdt = OffsetDateTime.of(2024, 5, 15, 10,
            30, 0, 0, ZoneOffset.UTC);
    TIMESTAMPTZ tzMock = Mockito.mock(TIMESTAMPTZ.class);
    when(tzMock.offsetDateTimeValue(eq(connection))).thenReturn(expectedOdt);
    when(resultSet.getString(eq(1))).thenReturn("2024-05-15 10:30:00 GMT");
    when(resultSet.getObject(eq(2))).thenReturn(tzMock);
    when(resultSet.getTimestamp(eq(3))).thenReturn(nowTs);
    when(resultSet.getTimestamp(eq(4))).thenReturn(null);
    when(resultSet.getObject(eq(5))).thenReturn(nowTs);
    when(resultSet.getTimestamp(eq(5), Mockito.any())).thenReturn(nowTs);
    when(resultSet.getTimestamp(eq(6))).thenReturn(nowTs);
    when(resultSet.getTimestamp(eq(7))).thenReturn(null);
    when(resultSet.getTimestamp(eq(8))).thenReturn(nowTs);

    dbRecord.handleField(resultSet, builder, tzStringField, 1,
            OracleSourceSchemaReader.TIMESTAMP_TZ, 0, 0);
    dbRecord.handleField(resultSet, builder, tzMicrosField, 2,
            OracleSourceSchemaReader.TIMESTAMP_TZ, 0, 0);
    dbRecord.handleField(resultSet, builder, tsDateTimeField, 3,
            Types.TIMESTAMP, 0, 0);
    dbRecord.handleField(resultSet, builder, tsNullDateTimeField, 4,
            Types.TIMESTAMP, 0, 0);
    dbRecord.handleField(resultSet, builder, tsMicrosField, 5,
            Types.TIMESTAMP, 0, 0);
    dbRecord.handleField(resultSet, builder, ltzDateTimeField, 6,
            OracleSourceSchemaReader.TIMESTAMP_LTZ, 0, 0);
    dbRecord.handleField(resultSet, builder, ltzNullDateTimeField, 7,
            OracleSourceSchemaReader.TIMESTAMP_LTZ, 0, 0);
    dbRecord.handleField(resultSet, builder, ltzMicrosField, 8,
            OracleSourceSchemaReader.TIMESTAMP_LTZ, 0, 0);

    StructuredRecord record = builder.build();
    Assert.assertEquals("2024-05-15 10:30:00 GMT", record.get("TZ_STR_COL"));
    Assert.assertEquals(expectedOdt.atZoneSameInstant(ZoneId.of("UTC")),
            record.getTimestamp("TZ_MICROS_COL"));
    Assert.assertEquals(nowTs.toLocalDateTime(), record.getDateTime("TS_DT_COL"));
    Assert.assertNull(record.getDateTime("TS_NULL_DT_COL"));
    Assert.assertNotNull(record.getTimestamp("TS_MICROS_COL"));
    Assert.assertNotNull(record.getDateTime("LTZ_DT_COL"));
    Assert.assertNull(record.getDateTime("LTZ_NULL_DT_COL"));
    Assert.assertNotNull(record.getTimestamp("LTZ_MICROS_COL"));
  }

  @Test
  public void handleField_uninitializedTimestampTzObject_throwsRuntimeException() throws Exception {
    Schema.Field tzMicrosField = Schema.Field.of("TZ_MICROS_COL",
            Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    Schema schema = Schema.recordOf("dbRecord", tzMicrosField);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    when(resultSet.getObject(eq(1))).thenReturn(new TIMESTAMPTZ());
    when(statement.getConnection()).thenReturn(null);

    Assert.assertThrows(RuntimeException.class, () ->
      dbRecord.handleField(resultSet, builder, tzMicrosField, 1,
              OracleSourceSchemaReader.TIMESTAMP_TZ, 0, 0));
  }

  @Test
  public void readFields_longRawAndStandardColumns_readsInExpectedOrder() throws Exception {
    OracleSourceDBRecord defaultRecord = new OracleSourceDBRecord();
    Schema.Field varcharField = Schema.Field.of("VARCHAR_COL", Schema.of(Schema.Type.STRING));
    Schema.Field longField = Schema.Field.of("LONG_COL", Schema.of(Schema.Type.STRING));
    Schema.Field longRawField = Schema.Field.of("LONG_RAW_COL", Schema.of(Schema.Type.BYTES));
    Schema schema = Schema.recordOf("dbRecord", varcharField, longField, longRawField);
    Configuration conf = new Configuration();
    conf.set(ConnectionConfigAccessor.OVERRIDE_SCHEMA, schema.toString());
    defaultRecord.setConf(conf);
    byte[] rawBytes = new byte[]{ 1, 2, 3 };
    when(resultSet.findColumn(eq("VARCHAR_COL"))).thenReturn(1);
    when(resultSet.findColumn(eq("LONG_COL"))).thenReturn(2);
    when(resultSet.findColumn(eq("LONG_RAW_COL"))).thenReturn(3);
    when(resultSetMetaData.getColumnType(eq(1))).thenReturn(Types.VARCHAR);
    when(resultSetMetaData.getColumnType(eq(2))).thenReturn(OracleSourceSchemaReader.LONG);
    when(resultSetMetaData.getColumnType(eq(3))).thenReturn(OracleSourceSchemaReader.LONG_RAW);
    when(resultSet.getObject(eq(1))).thenReturn("standardText");
    when(resultSet.getString(eq(2))).thenReturn("longText");
    when(resultSet.getBytes(eq(3))).thenReturn(rawBytes);

    defaultRecord.readFields(resultSet);

    StructuredRecord builtRecord = defaultRecord.getRecord();
    Assert.assertEquals("standardText", builtRecord.get("VARCHAR_COL"));
    Assert.assertEquals("longText", builtRecord.get("LONG_COL"));
    Assert.assertArrayEquals(rawBytes, builtRecord.get("LONG_RAW_COL"));
  }

  @Test
  public void write_invalidTimestampTzFormat_throwsInvalidStageException() throws Exception {
    when(preparedStatement.getConnection()).thenReturn(connection);
    Schema schema = Schema.recordOf("tzRecord",
            Schema.Field.of("TZ_STR", Schema.of(Schema.Type.STRING)));
    StructuredRecord record = StructuredRecord.builder(schema).set("TZ_STR", "invalid").build();
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(record,
            Collections.singletonList(new ColumnType("TZ_STR", "TIMESTAMPTZ",
                    OracleSourceSchemaReader.TIMESTAMP_TZ)));

    Assert.assertThrows(InvalidStageException.class, () -> dbRecord.write(preparedStatement));
  }

  @Test
  public void write_invalidTimestampLtzFormat_throwsInvalidStageException() throws Exception {
    when(preparedStatement.getConnection()).thenReturn(connection);
    Schema schema = Schema.recordOf("ltzRecord", Schema.Field.of("LTZ_DT",
            Schema.of(Schema.LogicalType.DATETIME)));
    StructuredRecord record = StructuredRecord.builder(schema).setDateTime("LTZ_DT",
                    LocalDateTime.of(2024, 5, 15, 10, 30)).build();
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(record,
            Collections.singletonList(new ColumnType("LTZ_DT",
                    "TIMESTAMPLTZ", OracleSourceSchemaReader.TIMESTAMP_LTZ)));

    Assert.assertThrows(InvalidStageException.class, () -> dbRecord.write(preparedStatement));
  }

  @Test
  public void write_isolatedConnectionWithStandardTimestamp_throwsInvalidStageException() throws Exception {
    ClassLoader isolatedLoader = new URLClassLoader(new URL[0], Connection.class.getClassLoader());
    Connection isolatedConnection = (Connection) Proxy.newProxyInstance(isolatedLoader,
            new Class<?>[]{ Connection.class },
            (proxy, method, args) -> null);
    when(preparedStatement.getConnection()).thenReturn(isolatedConnection);
    Schema schema = Schema.recordOf("tsRecord", Schema.Field.of("TS_DT",
            Schema.of(Schema.LogicalType.DATETIME)));
    StructuredRecord record = StructuredRecord.builder(schema).setDateTime("TS_DT",
                    LocalDateTime.of(2024, 5, 15, 10, 30)).build();
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(record,
            Collections.singletonList(new ColumnType("TS_DT", "TIMESTAMP", Types.TIMESTAMP)));

    Assert.assertThrows(InvalidStageException.class, () -> dbRecord.write(preparedStatement));
  }

  @Test
  public void write_isolatedConnectionWithTimestampLtz_throwsInvalidStageException() throws Exception {
    ClassLoader isolatedLoader = new URLClassLoader(new URL[0], Connection.class.getClassLoader());
    Connection isolatedConnection = (Connection) Proxy.newProxyInstance(isolatedLoader,
            new Class<?>[]{ Connection.class },
            (proxy, method, args) -> null);
    when(preparedStatement.getConnection()).thenReturn(isolatedConnection);
    Schema schema = Schema.recordOf("ltzRecord", Schema.Field.of("LTZ_DT",
            Schema.of(Schema.LogicalType.DATETIME)));
    StructuredRecord record = StructuredRecord.builder(schema).setDateTime("LTZ_DT",
                    LocalDateTime.of(2024, 5, 15, 10, 30)).build();
    OracleSourceDBRecord dbRecord = new OracleSourceDBRecord(record,
            Collections.singletonList(new ColumnType("LTZ_DT", "TIMESTAMPLTZ",
                    OracleSourceSchemaReader.TIMESTAMP_LTZ)));

    Assert.assertThrows(InvalidStageException.class, () -> dbRecord.write(preparedStatement));
  }

  @Test
  public void writeBytes_byteBufferAndByteArray_setsBytesOnStatement() throws Exception {
    byte[] rawBytes = new byte[]{ 5, 6, 7 };

    dbRecord.writeBytes(preparedStatement, 0, 1, ByteBuffer.wrap(rawBytes));
    dbRecord.writeBytes(preparedStatement, 1, 2, rawBytes);

    Assert.assertNotNull(preparedStatement);
  }
}

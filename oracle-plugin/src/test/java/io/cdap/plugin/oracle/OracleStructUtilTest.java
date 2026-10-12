/*
 * Copyright © 2024 Cask Data, Inc.
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
import oracle.jdbc.internal.OracleBfile;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit Test class for the OracleStructUtil
 */
@RunWith(MockitoJUnitRunner.class)
public class OracleStructUtilTest {

    @Mock
    private OracleSourceDBRecord sourceRecord;

    @Mock
    private Connection connection;

    @Test
    public void populateRecordField_nullValue_setsNullValue() throws Exception {
        Schema.Field field = Schema.Field.of("ID", Schema.nullableOf(Schema.of(Schema.Type.STRING)));
        Schema schema = Schema.recordOf("testRecord", field);
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);

        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, field, null);

        StructuredRecord record = builder.build();
        Assert.assertNull(record.get("ID"));
    }

    @Test
    public void populateRecordField_complexTypes_setsFieldsCorrectly() throws Exception {
        Schema innerSchema = Schema.recordOf("inner", Schema.Field.of("INNER_COL",
                Schema.of(Schema.Type.STRING)));
        Schema.Field structField = Schema.Field.of("STRUCT_FIELD", innerSchema);
        Schema.Field clobField = Schema.Field.of("CLOB_FIELD", Schema.of(Schema.Type.STRING));
        Schema.Field blobField = Schema.Field.of("BLOB_FIELD", Schema.of(Schema.Type.BYTES));
        Schema schema = Schema.recordOf("complexRecord", structField, clobField, blobField);
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        Struct structMock = Mockito.mock(Struct.class);
        StructuredRecord innerRecord = StructuredRecord.builder(innerSchema).set("INNER_COL", "val").build();
        when(sourceRecord.convertStructToRecord(eq(structMock), eq(innerSchema), eq(connection)))
                .thenReturn(innerRecord);
        Clob clobMock = Mockito.mock(Clob.class);
        when(clobMock.length()).thenReturn(4L);
        when(clobMock.getSubString(1, 4)).thenReturn("text");
        Blob blobMock = Mockito.mock(Blob.class);
        byte[] blobBytes = new byte[]{1, 2, 3};
        when(blobMock.length()).thenReturn(3L);
        when(blobMock.getBytes(1, 3)).thenReturn(blobBytes);

        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, structField, structMock);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, clobField, clobMock);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, blobField, blobMock);

        StructuredRecord record = builder.build();
        Assert.assertEquals(innerRecord, record.get("STRUCT_FIELD"));
        Assert.assertEquals("text", record.get("CLOB_FIELD"));
        Assert.assertArrayEquals(blobBytes, record.get("BLOB_FIELD"));
    }

    @Test
    public void populateRecordField_numericConversions_setsFieldsCorrectly() throws Exception {
        Schema.Field decimalField = Schema.Field.of("DEC_FIELD", Schema.decimalOf(10, 2));
        Schema.Field doubleField = Schema.Field.of("DOUBLE_FIELD", Schema.of(Schema.Type.DOUBLE));
        Schema.Field intField = Schema.Field.of("INT_FIELD", Schema.of(Schema.Type.INT));
        Schema.Field stringField = Schema.Field.of("STRING_FIELD", Schema.of(Schema.Type.STRING));
        Schema schema = Schema.recordOf("numericRecord", decimalField, doubleField,
                intField, stringField);
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        BigDecimal bigDecimalVal = new BigDecimal("123.456");

        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, decimalField, bigDecimalVal);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, doubleField, bigDecimalVal);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, intField, bigDecimalVal);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, stringField, bigDecimalVal);

        StructuredRecord record = builder.build();
        Assert.assertEquals(new BigDecimal("123.46"), record.getDecimal("DEC_FIELD"));
        Assert.assertEquals(123.456, (Double) record.get("DOUBLE_FIELD"), 0.0001);
        Assert.assertEquals(Integer.valueOf(123), record.get("INT_FIELD"));
        Assert.assertEquals("123.456", record.get("STRING_FIELD"));
    }

    @Test
    public void populateRecordField_temporalTypes_setsFieldsCorrectly() throws Exception {
        Schema.Field datetimeTsField = Schema.Field.of("DT_TS_FIELD",
                Schema.of(Schema.LogicalType.DATETIME));
        Schema.Field microsOdtField = Schema.Field.of("MICROS_ODT_FIELD",
                Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
        Schema.Field datetimeOdtField = Schema.Field.of("DT_ODT_FIELD",
                Schema.of(Schema.LogicalType.DATETIME));
        Schema.Field stringOdtField = Schema.Field.of("STR_ODT_FIELD", Schema.of(Schema.Type.STRING));
        Schema schema = Schema.recordOf("temporalRecord", datetimeTsField,
                microsOdtField, datetimeOdtField, stringOdtField);
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        Timestamp timestamp = Timestamp.valueOf("2024-05-15 10:30:00");
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2024, 5, 15,
                10, 30, 0, 0, ZoneOffset.ofHours(5));

        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, datetimeTsField, timestamp);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, microsOdtField, offsetDateTime);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, datetimeOdtField, offsetDateTime);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, stringOdtField, offsetDateTime);

        StructuredRecord record = builder.build();
        Assert.assertEquals(LocalDateTime.parse("2024-05-15T10:30:00"),
                record.getDateTime("DT_TS_FIELD"));
        Assert.assertEquals(offsetDateTime.atZoneSameInstant(ZoneId.of("UTC")),
                record.getTimestamp("MICROS_ODT_FIELD"));
        Assert.assertEquals(offsetDateTime.toLocalDateTime(),
                record.getDateTime("DT_ODT_FIELD"));
        Assert.assertEquals(offsetDateTime.atZoneSameInstant(ZoneId.of("UTC")).toString(),
                record.get("STR_ODT_FIELD"));
    }

    @Test
    public void populateRecordField_xmlAndIntervalAndBfileTypes_setsFieldsCorrectly() throws Exception {
        Schema.Field xmlField = Schema.Field.of("XML_FIELD", Schema.nullableOf(Schema.of(Schema.Type.STRING)));
        Schema.Field intervalDsField = Schema.Field.of("IDS_FIELD", Schema.of(Schema.Type.STRING));
        Schema.Field intervalYmField = Schema.Field.of("IYM_FIELD", Schema.of(Schema.Type.STRING));
        Schema.Field bfileField = Schema.Field.of("BFILE_FIELD", Schema.of(Schema.Type.BYTES));
        Schema.Field defaultField = Schema.Field.of("STR_FIELD", Schema.of(Schema.Type.STRING));
        Schema schema = Schema.recordOf("oracleTypesRecord", xmlField, intervalDsField,
                intervalYmField, bfileField, defaultField);
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        OracleSourceDBRecord realSourceRecord = new OracleSourceDBRecord(null, null);
        SQLXML sqlXmlMock = Mockito.mock(SQLXML.class);
        when(sqlXmlMock.getString()).thenReturn("<root/>");
        INTERVALDS intervalDs = new INTERVALDS("23 3:2:10.0");
        INTERVALYM intervalYm = new INTERVALYM("300-5");
        OracleBfile bfileMock = Mockito.mock(OracleBfile.class);
        byte[] bfileBytes = new byte[]{4, 5, 6};
        when(sourceRecord.getBfileBytes(eq(bfileMock), eq("BFILE_FIELD"))).thenReturn(bfileBytes);

        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, xmlField, sqlXmlMock);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, intervalDsField, intervalDs);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, intervalYmField, intervalYm);
        OracleStructUtil.populateRecordField(sourceRecord, connection, builder, bfileField, bfileMock);
        OracleStructUtil.populateRecordField(realSourceRecord, connection, builder,
                defaultField, "plainString");

        StructuredRecord record = builder.build();
        Assert.assertEquals("<root/>", record.get("XML_FIELD"));
        Assert.assertEquals(intervalDs.toString(), record.get("IDS_FIELD"));
        Assert.assertEquals(intervalYm.toString(), record.get("IYM_FIELD"));
        Assert.assertArrayEquals(bfileBytes, record.get("BFILE_FIELD"));
        Assert.assertEquals("plainString", record.get("STR_FIELD"));
    }

    @Test
    public void populateRecordField_additionalNumericAndTemporalFields_setsFieldsCorrectly() throws Exception {
        Schema.Field floatField = Schema.Field.of("FLOAT_FIELD", Schema.of(Schema.Type.FLOAT));
        Schema.Field longField = Schema.Field.of("LONG_FIELD", Schema.of(Schema.Type.LONG));
        Schema.Field dateTsField = Schema.Field.of("DATE_TS_FIELD", Schema.of(Schema.LogicalType.DATE));
        Schema.Field strTsField = Schema.Field.of("STR_TS_FIELD", Schema.of(Schema.Type.STRING));
        Schema.Field fallbackTsField = Schema.Field.of("FALLBACK_TS_FIELD",
                Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
        Schema.Field millisOdtField = Schema.Field.of("MILLIS_ODT_FIELD",
                Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS));
        Schema schema = Schema.recordOf("extraBranchesRecord", floatField, longField,
                dateTsField, strTsField, fallbackTsField, millisOdtField);
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        OracleSourceDBRecord realSourceRecord = new OracleSourceDBRecord(null, null);
        BigDecimal bigDecimalVal = new BigDecimal("123.45");
        Timestamp timestamp = Timestamp.valueOf("2024-05-15 10:30:00");
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2024, 5, 15,
                10, 30, 0, 0, ZoneOffset.ofHours(5));

        OracleStructUtil.populateRecordField(realSourceRecord, connection, builder, floatField, bigDecimalVal);
        OracleStructUtil.populateRecordField(realSourceRecord, connection, builder, longField, bigDecimalVal);
        OracleStructUtil.populateRecordField(realSourceRecord, connection, builder, dateTsField, timestamp);
        OracleStructUtil.populateRecordField(realSourceRecord, connection, builder, strTsField, timestamp);
        OracleStructUtil.populateRecordField(realSourceRecord, connection, builder, fallbackTsField, timestamp);
        OracleStructUtil.populateRecordField(realSourceRecord, connection, builder, millisOdtField, offsetDateTime);

        StructuredRecord record = builder.build();
        Assert.assertEquals(123.45f, record.get("FLOAT_FIELD"), 0.001f);
        Assert.assertEquals(Long.valueOf(123L), record.get("LONG_FIELD"));
        Assert.assertEquals(LocalDate.of(2024, 5, 15), record.getDate("DATE_TS_FIELD"));
        Assert.assertEquals(timestamp.toString(), record.get("STR_TS_FIELD"));
        Assert.assertEquals(timestamp.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)),
                record.getTimestamp("FALLBACK_TS_FIELD"));
        Assert.assertEquals(offsetDateTime.atZoneSameInstant(ZoneId.of("UTC")),
                record.getTimestamp("MILLIS_ODT_FIELD"));
    }
}

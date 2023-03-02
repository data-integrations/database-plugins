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

package io.cdap.plugin.postgres;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.util.DBUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit Test class for the PostgresDBRecord
 */
@RunWith(MockitoJUnitRunner.class)
public class PostgresDBRecordUnitTest {
    @Test
    public void validateTimestampType() throws SQLException {
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2023, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC);
        ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
        when(metaData.getColumnTypeName(eq(0))).thenReturn("timestamp");

        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.getTimestamp(eq(0), eq(DBUtils.PURE_GREGORIAN_CALENDAR)))
                .thenReturn(Timestamp.from(offsetDateTime.toInstant()));

        Schema.Field field1 = Schema.Field.of("field1", Schema.of(Schema.LogicalType.DATETIME));
        Schema schema = Schema.recordOf(
                "dbRecord",
                field1
        );
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);

        PostgresDBRecord dbRecord = new PostgresDBRecord(null, null, null, null);
        dbRecord.handleField(resultSet, builder, field1, 0, Types.TIMESTAMP, 0, 0);
        StructuredRecord record = builder.build();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getDateTime("field1"));
        Assert.assertEquals(record.getDateTime("field1").toInstant(ZoneOffset.UTC), offsetDateTime.toInstant());

        // Validate backward compatibility

        field1 = Schema.Field.of("field1", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
        schema = Schema.recordOf(
            "dbRecord",
            field1
        );
        builder = StructuredRecord.builder(schema);
        dbRecord.handleField(resultSet, builder, field1, 0, Types.TIMESTAMP, 0, 0);
        record = builder.build();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getTimestamp("field1"));
        Assert.assertEquals(record.getTimestamp("field1").toInstant(), offsetDateTime.toInstant());
    }

    @Test
    public void validateTimestampTZType() throws SQLException {
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2023, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC);
        ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
        when(metaData.getColumnTypeName(eq(0))).thenReturn("timestamptz");

        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.getObject(eq(0), eq(OffsetDateTime.class))).thenReturn(offsetDateTime);

        Schema.Field field1 = Schema.Field.of("field1", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
        Schema schema = Schema.recordOf(
                "dbRecord",
                field1
        );
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);

        PostgresDBRecord dbRecord = new PostgresDBRecord(null, null, null, null);
        dbRecord.handleField(resultSet, builder, field1, 0, Types.TIMESTAMP, 0, 0);
        StructuredRecord record = builder.build();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getTimestamp("field1", ZoneId.of("UTC")));
        Assert.assertEquals(record.getTimestamp("field1", ZoneId.of("UTC")).toInstant(), offsetDateTime.toInstant());
    }
}

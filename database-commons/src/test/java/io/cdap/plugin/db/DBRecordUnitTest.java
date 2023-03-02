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

package io.cdap.plugin.db;

import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockPreparedStatement;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the DBRecord class
 */
public class DBRecordUnitTest {

    @Test
    public void validateTimeStampWriteOperation() throws SQLException {
        String instantStr1 = "0001-01-01T01:00:00Z";
        String instantStr2 = "2023-01-01T01:00:00Z";

        ZonedDateTime tStamp1 = Instant.parse(instantStr1).atZone(ZoneId.of("UTC"));
        ZonedDateTime tStamp2 = Instant.parse(instantStr2).atZone(ZoneId.of("UTC"));

        StructuredRecord record = Mockito.mock(StructuredRecord.class);
        Mockito.when(record.get(Mockito.eq("COL1"))).thenReturn(tStamp1);
        Mockito.when(record.getTimestamp(Mockito.eq("COL1"))).thenReturn(tStamp1);
        Mockito.when(record.get(Mockito.eq("COL2"))).thenReturn(tStamp2);
        Mockito.when(record.getTimestamp(Mockito.eq("COL2"))).thenReturn(tStamp2);

        List<ColumnType> columnTypes = new ArrayList<ColumnType>();
        columnTypes.add(new ColumnType("COL1", "timestamp", Types.TIMESTAMP));
        columnTypes.add(new ColumnType("COL2", "timestamp", Types.TIMESTAMP));

        MockPreparedStatement preparedStatement = new MockPreparedStatement(new MockConnection(), "sql");

        Schema schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        Schema.Field field1 = Schema.Field.of("COL1", schema);
        Schema.Field field2 = Schema.Field.of("COL2", schema);

        DBRecord dbRecord = new DBRecord(record, columnTypes);
        dbRecord.writeToDB(preparedStatement, field1, 0);
        dbRecord.writeToDB(preparedStatement, field2, 1);

        Assert.assertTrue(Timestamp.valueOf(tStamp1.toLocalDateTime()).equals(preparedStatement.getParameter(1)));
        Assert.assertTrue(Timestamp.valueOf(tStamp2.toLocalDateTime()).equals(preparedStatement.getParameter(2)));
    }
}

/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.teradata;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.db.dbrecordwriter.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Writable class for Teradata Source/Sink.
 */
public class TeradataDBRecord extends DBRecord {

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public TeradataDBRecord() {
  }

  /**
   * Used to construct a DBRecord from a StructuredRecord in the ETL Pipeline.
   *
   * @param record the {@link StructuredRecord} to construct the {@link TeradataDBRecord} from
   */
  public TeradataDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  @Override
  protected void writeBytes(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue)
    throws SQLException {
    byte[] byteValue = fieldValue instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) fieldValue) : (byte[]) fieldValue;
    // handles BLOB, BINARY, VARBINARY
    stmt.setBytes(sqlIndex, byteValue);
  }

  @Override
  protected void setField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                          int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    Object original = resultSet.getObject(columnIndex);
    if (original != null && sqlType == Types.NUMERIC) {
      BigDecimal decimal = (BigDecimal) original;
      recordBuilder.setDecimal(field.getName(), decimal.setScale(sqlScale, RoundingMode.HALF_EVEN));
    } else {
      super.setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }
}

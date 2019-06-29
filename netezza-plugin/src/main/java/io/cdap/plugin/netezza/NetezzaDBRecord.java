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

package io.cdap.plugin.netezza;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Set;

/**
 * Writable class for Netezza Source/Sink
 */
public class NetezzaDBRecord extends DBRecord {

  private static final int INTERVAL = 101;

  private static final Set<Integer> netezzaTypes = ImmutableSet.of(
    Types.VARBINARY,
    INTERVAL
  );

  public NetezzaDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public NetezzaDBRecord() {}

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (netezzaTypes.contains(sqlType)) {
      handleNetezzaSpecificType(resultSet, recordBuilder, field, columnIndex, sqlType);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  private void handleNetezzaSpecificType(ResultSet resultSet, StructuredRecord.Builder recordBuilder,
                                         Schema.Field field, int columnIndex, int sqlType) throws SQLException {

    switch (sqlType) {
      case Types.VARBINARY:
        recordBuilder.set(field.getName(), resultSet.getBytes(columnIndex));
        break;
      case INTERVAL:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
    }
  }
}

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

package co.cask.netezza;

import co.cask.DBRecord;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * Writable class for Oracle Source/Sink
 */
public class NetezzaDBRecord extends DBRecord {

  private static final int INTERVAL = 101;

  private static final Set<Integer> netezzaTypes = ImmutableSet.of(
    Types.VARBINARY,
    INTERVAL
  );

  public NetezzaDBRecord(StructuredRecord record, int[] columnTypes) {
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
                             int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (netezzaTypes.contains(sqlType)) {
      handleOracleSpecificType(resultSet, recordBuilder, field, sqlType);
    } else {
      setField(resultSet, recordBuilder, field, sqlType, sqlPrecision, sqlScale);
    }
  }

  private void handleOracleSpecificType(ResultSet resultSet,
                                        StructuredRecord.Builder recordBuilder, Schema.Field field,
                                        int sqlType) throws SQLException {

    Object original = resultSet.getObject(field.getName());

    switch (sqlType) {
      case Types.VARBINARY:
        original = resultSet.getBytes(field.getName());
        recordBuilder.set(field.getName(), original);
        break;
      case INTERVAL:
        recordBuilder.set(field.getName(), original.toString());
        break;
    }
  }
}

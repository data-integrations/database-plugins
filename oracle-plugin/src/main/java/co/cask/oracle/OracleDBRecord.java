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

package co.cask.oracle;

import co.cask.DBRecord;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * Writable class for Oracle Source/Sink
 */
public class OracleDBRecord extends DBRecord {

  private static final int INTERVAL_YM = -103;
  private static final int INTERVAL_DS = -104;

  private static final Set<Integer> oracleTypes = ImmutableSet.of(
    INTERVAL_DS,
    INTERVAL_YM
  );

  public OracleDBRecord(StructuredRecord record, int[] columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public OracleDBRecord() {}

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (oracleTypes.contains(sqlType)) {
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
      case INTERVAL_YM:
      case INTERVAL_DS:
        recordBuilder.set(field.getName(), original.toString());
        break;
    }
  }
}

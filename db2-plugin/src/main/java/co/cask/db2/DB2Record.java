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

package co.cask.db2;

import co.cask.DBRecord;
import co.cask.SchemaReader;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Writable class for DB2 Source/Sink.
 */
public class DB2Record extends DBRecord {

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public DB2Record() {
  }

  public DB2Record(StructuredRecord build, int[] columnTypes) {
    super(build, columnTypes);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new DB2SchemaReader();
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (DB2SchemaReader.DB2_TYPES.contains(sqlType)) {
      handleSpecificType(resultSet, recordBuilder, field,  resultSet.findColumn(field.getName()));
    } else {
      setField(resultSet, recordBuilder, field, sqlType, sqlPrecision, sqlScale);
    }
  }

  private void handleSpecificType(ResultSet resultSet,
                                  StructuredRecord.Builder recordBuilder,
                                  Schema.Field field, int index) throws SQLException {

    ResultSetMetaData metaData = resultSet.getMetaData();

    String columnTypeName = metaData.getColumnTypeName(index);

    if (DB2SchemaReader.DB2_DECFLOAT.equals(columnTypeName)) {
      recordBuilder.set(field.getName(), resultSet.getBigDecimal("DECFLOAT_COL").doubleValue());
    }
  }
}

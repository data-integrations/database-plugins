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

package io.cdap.plugin.memsql;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.DBRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Writable class for MemSQL Source/Sink
 */
public class MemsqlDBRecord extends DBRecord {

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
      : field.getSchema().getType();

    // In MemqSQL bool stores as tinyint
    if (fieldType == Schema.Type.BOOLEAN && sqlType == Types.TINYINT) {
      Integer value = resultSet.getInt(columnIndex);
      recordBuilder.set(field.getName(), value > 0);
    } else if (sqlType == Types.BIT) {
      Boolean value = resultSet.getBoolean(columnIndex);
      recordBuilder.set(field.getName(), value);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }
}

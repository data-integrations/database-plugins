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

package co.cask.auroradb.postgres;

import co.cask.DBRecord;
import co.cask.SchemaReader;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Writable class for Aurora DB PostgreSQL Source/Sink
 */
public class AuroraPostgresDBRecord extends DBRecord {

  public AuroraPostgresDBRecord(StructuredRecord record, int[] columnTypes) {
    super(record, columnTypes);
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public AuroraPostgresDBRecord() {}

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (AuroraPostgresSchemaReader.POSTGRES_TYPES.contains(sqlType)) {
      handleSpecificType(resultSet, recordBuilder, field);
    } else {
      setField(resultSet, recordBuilder, field, sqlType, sqlPrecision, sqlScale);
    }
  }

  private void handleSpecificType(ResultSet resultSet,
                                  StructuredRecord.Builder recordBuilder, Schema.Field field) throws SQLException {
    setFieldAccordingToSchema(resultSet, recordBuilder, field);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new AuroraPostgresSchemaReader();
  }
}

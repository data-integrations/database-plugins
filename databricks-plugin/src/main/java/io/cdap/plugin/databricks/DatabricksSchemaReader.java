/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.databricks;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Databricks Schema Reader class
 */
public class DatabricksSchemaReader extends CommonSchemaReader {

  private final String sessionID;

  public DatabricksSchemaReader() {
    this(null);
  }

  public DatabricksSchemaReader(String sessionID) {
    super();
    this.sessionID = sessionID;
  }

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    String typeName = metadata.getColumnTypeName(index);

    if (typeName != null) {
      if (typeName.equalsIgnoreCase("INT") || typeName.equalsIgnoreCase("INTEGER") ||
          typeName.equalsIgnoreCase("SMALLINT") || typeName.equalsIgnoreCase("TINYINT")) {
        return Schema.of(Schema.Type.INT);
      }
      if (typeName.equalsIgnoreCase("BIGINT")) {
        return Schema.of(Schema.Type.LONG);
      }
      if (typeName.equalsIgnoreCase("TIMESTAMP") || typeName.equalsIgnoreCase("TIMESTAMP_NTZ") ||
          typeName.equalsIgnoreCase("TIMESTAMPTZ")) {
        return Schema.of(Schema.LogicalType.DATETIME);
      }
      if (typeName.equalsIgnoreCase("DATE")) {
        return Schema.of(Schema.LogicalType.DATE);
      }
      if (typeName.equalsIgnoreCase("VARIANT") || typeName.equalsIgnoreCase("ARRAY") ||
          typeName.equalsIgnoreCase("MAP") || typeName.equalsIgnoreCase("STRUCT") ||
          typeName.equalsIgnoreCase("JSON")) {
        return Schema.of(Schema.Type.STRING);
      }
    }

    return super.getSchema(metadata, index);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    if (sessionID == null) {
      return false;
    }
    String columnName = metadata.getColumnName(index);
    return ("c_" + sessionID).equals(columnName) || ("sqn_" + sessionID).equals(columnName);
  }
}

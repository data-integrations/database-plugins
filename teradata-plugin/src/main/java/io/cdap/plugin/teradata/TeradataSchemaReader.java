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

package io.cdap.plugin.teradata;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * Teradata schema reader.
 */
public class TeradataSchemaReader extends CommonSchemaReader {
  public static final Set<String> TERADATA_STRING_TYPES = ImmutableSet.of(
    "INTERVAL YEAR",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL MONTH",
    "INTERVAL DAY",
    "INTERVAL DAY TO HOUR",
    "INTERVAL DAY TO MINUTE",
    "INTERVAL DAY TO SECOND",
    "INTERVAL HOUR",
    "INTERVAL HOUR TO MINUTE",
    "INTERVAL HOUR TO SECOND",
    "INTERVAL MINUTE",
    "INTERVAL MINUTE TO SECOND",
    "INTERVAL SECOND"
  );

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int sqlType = metadata.getColumnType(index);
    String sqlTypeName = metadata.getColumnTypeName(index);

    // Teradata interval types are mapping to String
    if (TERADATA_STRING_TYPES.contains(sqlTypeName)) {
      return Schema.of(Schema.Type.STRING);
    }

    // In Teradata FLOAT and DOUBLE are same types
    if (sqlType == Types.FLOAT) {
      return Schema.of(Schema.Type.DOUBLE);
    }

    return super.getSchema(metadata, index);
  }
}

/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.amazon.redshift;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Set;

/**
 * Redshift Schema Reader class
 */
public class RedshiftSchemaReader extends CommonSchemaReader {

  private static final Logger LOG = LoggerFactory.getLogger(RedshiftSchemaReader.class);

  public static final Set<String> STRING_MAPPED_REDSHIFT_TYPES_NAMES = ImmutableSet.of(
    "timetz", "money"
  );

  private final String sessionID;

  public RedshiftSchemaReader() {
    this(null);
  }

  public RedshiftSchemaReader(String sessionID) {
    super();
    this.sessionID = sessionID;
  }

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    String typeName = metadata.getColumnTypeName(index);
    int columnType = metadata.getColumnType(index);

    if (STRING_MAPPED_REDSHIFT_TYPES_NAMES.contains(typeName)) {
      return Schema.of(Schema.Type.STRING);
    }
    if (typeName.equalsIgnoreCase("INT")) {
      return Schema.of(Schema.Type.INT);
    }
    if (typeName.equalsIgnoreCase("BIGINT")) {
      return Schema.of(Schema.Type.LONG);
    }

    // If it is a numeric type without precision then use the Schema of String to avoid any precision loss
    if (Types.NUMERIC == columnType) {
      int precision = metadata.getPrecision(index);
      if (precision == 0) {
        LOG.warn(String.format("Field '%s' is a %s type without precision and scale, "
                                 + "converting into STRING type to avoid any precision loss.",
                               metadata.getColumnName(index),
                               metadata.getColumnTypeName(index)));
        return Schema.of(Schema.Type.STRING);
      }
    }

    if (typeName.equalsIgnoreCase("timestamp")) {
      return Schema.of(Schema.LogicalType.DATETIME);
    }

    return super.getSchema(metadata, index);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    if (sessionID == null) {
      return false;
    }
    return metadata.getColumnName(index).equals("c_" + sessionID) ||
      metadata.getColumnName(index).equals("sqn_" + sessionID);
  }

  @Override
  public List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      if (shouldIgnoreColumn(metadata, i)) {
        continue;
      }
      String columnName = metadata.getColumnName(i);
      Schema columnSchema = getSchema(metadata, i);
      // Setting up schema as nullable as cdata driver doesn't provide proper information about isNullable.
      columnSchema = Schema.nullableOf(columnSchema);
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

}

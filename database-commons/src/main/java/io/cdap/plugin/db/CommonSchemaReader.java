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

package io.cdap.plugin.db;

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.db.DBUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Common schema reader for mapping non specific DB types.
 */
public class CommonSchemaReader implements SchemaReader {

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
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    return DBUtils.getSchemaReader(null, null, null).getSchema(metadata, index);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    return false;
  }
}

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

package co.cask.postgres;

import co.cask.CommonSchemaReader;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * PostgreSQL schema reader.
 */
public class PostgresSchemaReader extends CommonSchemaReader {

  public static final Set<Integer> POSTGRES_TYPES = ImmutableSet.of(
    Types.OTHER, Types.ARRAY, Types.SQLXML
  );

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    if (POSTGRES_TYPES.contains(metadata.getColumnType(index))) {
      return readSchema(metadata.getColumnTypeName(index), metadata.getColumnType(index));
    }

    return super.getSchema(metadata, index);
  }

  private Schema readSchema(String columnTypeName, int sqlType) throws SQLException {
      return Schema.of(Schema.Type.STRING);
  }
}

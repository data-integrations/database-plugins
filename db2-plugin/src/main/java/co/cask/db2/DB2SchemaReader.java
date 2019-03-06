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

import co.cask.CommonSchemaReader;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * DB2 Schema reader.
 */
public class DB2SchemaReader extends CommonSchemaReader {

  public static final String DB2_DECFLOAT = "DECFLOAT";

  public static final Set<Integer> DB2_TYPES = ImmutableSet.of(Types.OTHER);

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    if (DB2_TYPES.contains(metadata.getColumnType(index)) && DB2_DECFLOAT.equals(metadata.getColumnTypeName(index))) {
      return Schema.of(Schema.Type.DOUBLE);
    } else {
      return super.getSchema(metadata, index);
    }
  }
}

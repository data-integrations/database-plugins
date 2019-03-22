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

import co.cask.CommonSchemaReader;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Set;

/**
 * Oracle schema reader.
 */
public class OracleSchemaReader extends CommonSchemaReader {
  /**
   *  Oracle type constants, from Oracle JDBC Implementation.
   */
  public static final int INTERVAL_YM = -103;
  public static final int INTERVAL_DS = -104;
  public static final int TIMESTAMP_TZ = -101;
  public static final int TIMESTAMP_LTZ = -102;

  public static final Set<Integer> ORACLE_TYPES = ImmutableSet.of(
    INTERVAL_DS,
    INTERVAL_YM,
    TIMESTAMP_TZ,
    TIMESTAMP_LTZ
  );

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int sqlType = metadata.getColumnType(index);
    String sqlColumnName = metadata.getColumnName(index);

    if (ORACLE_TYPES.contains(sqlType)) {
      if (sqlType == TIMESTAMP_LTZ || sqlType == TIMESTAMP_TZ) {
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      }
      return Schema.of(Schema.Type.STRING);
    } else {
      return super.getSchema(metadata, index);
    }
  }
}

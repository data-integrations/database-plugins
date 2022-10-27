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

package io.cdap.plugin.oracle;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * Oracle Source schema reader.
 */
public class OracleSourceSchemaReader extends CommonSchemaReader {
  /**
   * Oracle type constants, from Oracle JDBC Implementation.
   */
  public static final int INTERVAL_YM = -103;
  public static final int INTERVAL_DS = -104;
  public static final int TIMESTAMP_TZ = -101;
  public static final int TIMESTAMP_LTZ = -102;
  public static final int BINARY_FLOAT = 100;
  public static final int BINARY_DOUBLE = 101;
  public static final int BFILE = -13;
  public static final int LONG = -1;
  public static final int LONG_RAW = -4;

  /**
   * Logger instance for Oracle Schema reader.
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleSourceSchemaReader.class);

  public static final Set<Integer> ORACLE_TYPES = ImmutableSet.of(
    INTERVAL_DS,
    INTERVAL_YM,
    TIMESTAMP_TZ,
    TIMESTAMP_LTZ,
    BINARY_FLOAT,
    BINARY_DOUBLE,
    BFILE,
    LONG,
    LONG_RAW,
    Types.NUMERIC,
    Types.DECIMAL
  );

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int sqlType = metadata.getColumnType(index);

    switch (sqlType) {
      case TIMESTAMP_TZ:
        return Schema.of(Schema.Type.STRING);
      case TIMESTAMP_LTZ:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case BINARY_FLOAT:
        return Schema.of(Schema.Type.FLOAT);
      case BINARY_DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);
      case BFILE:
      case LONG_RAW:
        return Schema.of(Schema.Type.BYTES);
      case INTERVAL_DS:
      case INTERVAL_YM:
      case LONG:
        return Schema.of(Schema.Type.STRING);
      case Types.NUMERIC:
      case Types.DECIMAL:
        // FLOAT and REAL are returned as java.sql.Types.NUMERIC but with value that is a java.lang.Double
        if (Double.class.getTypeName().equals(metadata.getColumnClassName(index))) {
          return Schema.of(Schema.Type.DOUBLE);
        } else {
          int precision = metadata.getPrecision(index); // total number of digits
          int scale = metadata.getScale(index); // digits after the decimal point
          // For a Number type without specified precision and scale, precision will be 0 and scale will be -127
          if (precision == 0) {
            // reference : https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
            precision = 38;
            scale = 0;
            LOG.warn(String.format("%s type with undefined precision and scale is detected, "
                    + "there may be a precision loss while running the pipeline. "
                    + "Please define an output precision and scale for field '%s' to avoid precision loss.",
                metadata.getColumnTypeName(index),
                metadata.getColumnName(index)));
          }
          return Schema.decimalOf(precision, scale);
        }
      default:
        return super.getSchema(metadata, index);
    }
  }
}

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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink support for Oracle database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(OracleConstants.PLUGIN_NAME)
@Description("Writes records to Oracle table. Each record will be written in a row in the table")
public class OracleSink extends AbstractDBSink {

  private final OracleSinkConfig oracleSinkConfig;

  public OracleSink(OracleSinkConfig oracleSinkConfig) {
    super(oracleSinkConfig);
    this.oracleSinkConfig = oracleSinkConfig;
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new OracleSinkDBRecord(output, columnTypes);
  }

  @Override
  protected boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    int sqlType = metadata.getColumnType(index);
    int precision = metadata.getPrecision(index);
    int scale = metadata.getScale(index);

    // Handle logical types first
    if (fieldLogicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
      return sqlType == OracleSinkSchemaReader.TIMESTAMP_LTZ ||
        super.isFieldCompatible(field, metadata, index);
    } else if (fieldLogicalType != null) {
      return super.isFieldCompatible(field, metadata, index);
    }

    switch (fieldType) {
      case FLOAT:
        return sqlType == OracleSinkSchemaReader.BINARY_FLOAT
          || super.isFieldCompatible(field, metadata, index);
      case BYTES:
        return sqlType == OracleSinkSchemaReader.BFILE
          || sqlType == OracleSinkSchemaReader.LONG_RAW
          || super.isFieldCompatible(field, metadata, index);
      case STRING:
        return sqlType == OracleSinkSchemaReader.LONG
          || sqlType == OracleSinkSchemaReader.TIMESTAMP_TZ
          || sqlType == OracleSinkSchemaReader.INTERVAL_DS
          || sqlType == OracleSinkSchemaReader.INTERVAL_YM
          || sqlType == Types.ROWID
          || super.isFieldCompatible(field, metadata, index);
      case INT:
        // Since all Oracle numeric types are based on NUMBER(i.e. INTEGER type is actually NUMBER(38, 0)) we won't be
        // able to use Oracle Sink Plugin with other sources. It's safe to write primitives as values of decimal
        // logical type in the case of valid precision.
        // The following schema compatibility is supported:
        // 1) Schema.Type.INT -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 10)
        if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC) {
          // With 10 digits we can represent Integer.MAX_VALUE.
          // It is equal to the value returned by (new BigDecimal(Integer.MAX_VALUE)).precision()
          return precision >= 10 && scale == 0;
        }
        return super.isFieldCompatible(field, metadata, index);
      case LONG:
        // Since all Oracle numeric types are based on NUMBER(i.e. INTEGER type is actually NUMBER(38, 0)) we won't be
        // able to use Oracle Sink Plugin with other sources. It's safe to write primitives as values of decimal
        // logical type in the case of valid precision.
        // The following schema compatibility is supported:
        // 2) Schema.Type.LONG -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 19)
        if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC) {
          // With 19 digits we can represent Long.MAX_VALUE.
          // It is equal to the value returned by (new BigDecimal(Long.MAX_VALUE)).precision()
          return precision >= 19 && scale == 0;
        }
        return super.isFieldCompatible(field, metadata, index);
      case DOUBLE:
        if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC) {
          // This is the only way to differentiate FLOAT/REAL columns from other numeric columns, that based on NUMBER.
          // Since in Oracle FLOAT is a subtype of the NUMBER data type, 'getColumnType' and 'getColumnTypeName' can not
          // be used.
          return Double.class.getTypeName().equals(metadata.getColumnClassName(index));
        }
        return sqlType == OracleSinkSchemaReader.BINARY_DOUBLE
          || super.isFieldCompatible(field, metadata, index);
      default:
        return super.isFieldCompatible(field, metadata, index);
    }
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSinkSchemaReader();
  }

  /**
   * Oracle action configuration.
   */
  public static class OracleSinkConfig extends DBSpecificSinkConfig {
    @Name(OracleConstants.DEFAULT_BATCH_VALUE)
    @Description("The default batch value that triggers an execution request.")
    @Nullable
    public Integer defaultBatchValue;

    @Name(OracleConstants.CONNECTION_TYPE)
    @Description("Whether to use an SID or Service Name when connecting to the database.")
    public String connectionType;

    @Override
    public String getConnectionString() {
      if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(this.connectionType)) {
        return String.format(OracleConstants.ORACLE_CONNECTION_SERVICE_NAME_STRING_FORMAT, host, port, database);
      }
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(OracleConstants.DEFAULT_BATCH_VALUE, String.valueOf(defaultBatchValue));
    }
  }
}

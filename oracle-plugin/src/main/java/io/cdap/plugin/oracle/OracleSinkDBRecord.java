package io.cdap.plugin.oracle;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Oracle Sink implementation {@link org.apache.hadoop.mapreduce.lib.db.DBWritable} and
 * {@link org.apache.hadoop.io.Writable}.
 */
public class OracleSinkDBRecord extends OracleSourceDBRecord {
  private static final Logger LOG = LoggerFactory.getLogger(OracleSinkDBRecord.class);

  public OracleSinkDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSinkSchemaReader();
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  public OracleSinkDBRecord() {
    // Required for Hadoop DBWritable
  }

  @Override
  public void write(PreparedStatement stmt) throws SQLException {
    LOG.debug("Writing record to PreparedStatement: {}", record);
    // This method is called by ETLDBOutputFormat.DBSinkRecordWriter
    // The operation type is not passed here, so we assume it's for MERGE
    // as this custom OracleETLDBOutputFormat is only used for MERGE.
    int paramIndex = 1;
    for (ColumnType columnType : columnTypes) {
      Schema.Field field = record.getSchema().getField(columnType.getName(), true);
      setField(stmt, paramIndex++, field, columnType);
    }
  }

  private void setField(PreparedStatement stmt, int paramIndex, Schema.Field field, ColumnType columnType)
    throws SQLException {
    Object val = record.get(field.getName());
    LOG.trace("Setting field: {}, index: {}, value: {}, sqlType: {}",
        field.getName(), paramIndex, val, columnType.getType());
    int sqlType = columnType.getType();
    Schema fieldSchema = field.getSchema();
    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

    if (val == null) {
      stmt.setNull(paramIndex, sqlType);
      return;
    }

    switch (fieldSchema.getType()) {
      case NULL:
        stmt.setNull(paramIndex, sqlType);
        break;
      case BOOLEAN:
        stmt.setBoolean(paramIndex, (Boolean) val);
        break;
      case INT:
        stmt.setInt(paramIndex, (Integer) val);
        break;
      case LONG:
        stmt.setLong(paramIndex, (Long) val);
        break;
      case FLOAT:
        stmt.setFloat(paramIndex, (Float) val);
        break;
      case DOUBLE:
        stmt.setDouble(paramIndex, (Double) val);
        break;
      case BYTES:
        if (fieldSchema.getLogicalType() == Schema.LogicalType.DECIMAL) {
          stmt.setBigDecimal(paramIndex, new BigDecimal(new String((byte[]) val)));
        } else {
          stmt.setBytes(paramIndex, (byte[]) val);
        }
        break;
      case STRING:
        stmt.setString(paramIndex, (String) val);
        break;
      case RECORD:
        // You might need to handle complex types based on your data
        throw new SQLException("Record types not fully supported in setField yet.");
      case ARRAY:
      case MAP:
      case UNION:
      case ENUM:
        throw new SQLException("Unsupported schema type: " + fieldSchema.getType() + " for field " + field.getName());
    }

    // Handle Logical Types
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DECIMAL:
          stmt.setBigDecimal(paramIndex, (BigDecimal) val);
          break;
        case DATE:
          stmt.setDate(paramIndex, java.sql.Date.valueOf(((java.time.LocalDate) val)));
          break;
        case TIME_MICROS:
        case TIME_MILLIS:
          stmt.setTime(paramIndex, java.sql.Time.valueOf(((java.time.LocalTime) val)));
          break;
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
          LocalDateTime localDateTime = (LocalDateTime) val;
          stmt.setTimestamp(paramIndex, Timestamp.valueOf(localDateTime));
          break;
        default:
          throw new SQLException("Unsupported logical type: " + logicalType + " for field " + field.getName());
      }
    }
  }
}

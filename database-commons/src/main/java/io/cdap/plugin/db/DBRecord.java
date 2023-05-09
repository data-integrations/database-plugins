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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.Lazy;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.sql.rowset.serial.SerialBlob;

/**
 * Writable class for DB Source/Sink
 *
 * @see org.apache.hadoop.mapreduce.lib.db.DBInputFormat DBInputFormat
 * @see org.apache.hadoop.mapreduce.lib.db.DBOutputFormat DBOutputFormat
 * @see DBWritable DBWritable
 */
public class DBRecord implements Writable, DBWritable, Configurable {
  protected StructuredRecord record;
  protected Configuration conf;
  private final Lazy<Schema> schema = new Lazy<>(this::computeSchema);

  /**
   * Need to cache column types to set fields of the input record on {@link PreparedStatement} in the right order.
   */
  protected List<ColumnType> columnTypes;

  /**
   * Need to cache column types to set fields of input record for where clause on {@link PreparedStatement}
   * in the right order.
   */
  protected List<ColumnType> modifiableColumnTypes;

  /**
   * Operation for the query to perform. By default, the query performs INSERT operation
   */
  protected Operation operationName;

  /**
   * List of fields that determines relation between tables during Update and Upsert operations.
   */
  protected String relationTableKey;

  /**
   * Used to construct a DBRecord from a StructuredRecord in the ETL Pipeline
   *
   * @param record the {@link StructuredRecord} to construct the {@link DBRecord} from
   */
  public DBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  /**
   * Used to construct a DBRecord from a StructuredRecord in the ETL Pipeline
   * and consists of operation name(Update and Upsert) with the keys to be updated
   *
   * @param record the {@link StructuredRecord} to construct the {@link DBRecord} from
   */
  public DBRecord(StructuredRecord record, List<ColumnType> columnTypes, Operation operationName,
                  String relationTableKey) {
    this.record = record;
    this.columnTypes = columnTypes;
    this.operationName = operationName;
    this.relationTableKey = relationTableKey;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public DBRecord() {
  }

  public void readFields(DataInput in) throws IOException {
    // no-op, since we may never need to support a scenario where you read a DBRecord from a non-RDBMS source
  }

  /**
   * @return the {@link StructuredRecord} contained in this object
   */
  public StructuredRecord getRecord() {
    return record;
  }

  /**
   * Returns the operation to be performed. By default, the value is supposed to be INSERT.
   * The values for operation can be INSERT, UPDATE, UPSERT
   * @return - Operation name
   */
  public Operation getOperationName() {
     return operationName == null ? Operation.INSERT :
      Operation.valueOf(operationName.toString().toUpperCase());
  }

  /**
   * Builds the {@link #record} using the specified {@link ResultSet}
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  public void readFields(ResultSet resultSet) throws SQLException {
    Schema schema = getSchema();
    ResultSetMetaData metadata = resultSet.getMetaData();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (int i = 0; i < schema.getFields().size(); i++) {
      Schema.Field field = schema.getFields().get(i);
      // Find the field index in the resultSet having the same name
      int columnIndex = resultSet.findColumn(field.getName());
      int sqlType = metadata.getColumnType(columnIndex);
      int sqlPrecision = metadata.getPrecision(columnIndex);
      int sqlScale = metadata.getScale(columnIndex);

      handleField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
    record = recordBuilder.build();
  }

  protected Schema getSchema() {
    return schema.getOrCompute();
  }

  private Schema computeSchema() {
    String schemaStr = new ConnectionConfigAccessor(conf).getSchema();
    if (schemaStr == null) {
      throw new IllegalStateException("Schema was not provided");
    }
    try {
      return Schema.parseJson(schemaStr);
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Unable to parse schema string %s", schemaStr), e);
    }
  }

  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }

  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
  }

  protected void setField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                          int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    Object o = DBUtils.transformValue(sqlType, sqlPrecision, sqlScale, resultSet, columnIndex);
    if (o instanceof Date) {
      recordBuilder.setDate(field.getName(), ((Date) o).toLocalDate());
    } else if (o instanceof Time) {
      recordBuilder.setTime(field.getName(), ((Time) o).toLocalTime());
    } else if (o instanceof Timestamp) {
      Instant instant = ((Timestamp) o).toInstant();
      recordBuilder.setTimestamp(field.getName(), instant.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
    } else if (o instanceof BigDecimal) {
      recordBuilder.setDecimal(field.getName(), (BigDecimal) o);
    } else if (o instanceof BigInteger) {
      Schema schema = field.getSchema();
      schema = schema.isNullable() ? schema.getNonNullable() : schema;
      if (schema.getType() == Schema.Type.LONG) {
        recordBuilder.set(field.getName(), ((BigInteger) o).longValueExact());
      } else {
        // BigInteger won't have any fraction part and scale is 0
        recordBuilder.setDecimal(field.getName(), new BigDecimal((BigInteger) o, 0));
      }
    } else {
      recordBuilder.set(field.getName(), o);
    }
  }

  protected void setFieldAccordingToSchema(ResultSet resultSet, StructuredRecord.Builder recordBuilder,
                                           Schema.Field field, int columnIndex) throws SQLException {
    Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
      : field.getSchema().getType();

    switch (fieldType) {
      case NULL:
        break;
      case STRING:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
      case BOOLEAN:
        recordBuilder.set(field.getName(), resultSet.getBoolean(columnIndex));
        break;
      case INT:
        recordBuilder.set(field.getName(), resultSet.getInt(columnIndex));
        break;
      case LONG:
        recordBuilder.set(field.getName(), resultSet.getLong(columnIndex));
        break;
      case FLOAT:
        recordBuilder.set(field.getName(), resultSet.getFloat(columnIndex));
        break;
      case DOUBLE:
        recordBuilder.set(field.getName(), resultSet.getDouble(columnIndex));
        break;
      case BYTES:
        recordBuilder.set(field.getName(), resultSet.getBytes(columnIndex));
        break;
    }
  }

  public void write(DataOutput out) throws IOException {
    Schema recordSchema = record.getSchema();
    List<Schema.Field> schemaFields = recordSchema.getFields();
    for (Schema.Field field : schemaFields) {
      writeToDataOut(out, field);
    }
  }

  /**
   * Writes the {@link #record} to the specified {@link PreparedStatement}
   *
   * @param stmt the {@link PreparedStatement} to write the {@link StructuredRecord} to
   */
  public void write(PreparedStatement stmt) throws SQLException {
    modifiableColumnTypes = new ArrayList<>(columnTypes);
    operationName = getOperationName();
    switch (operationName) {
      case INSERT:
        insertOperation(stmt);
        break;
      case UPDATE:
        updateOperation(stmt);
        break;
      case UPSERT:
        upsertOperation(stmt);
        break;
    }
  }

  protected void insertOperation(PreparedStatement stmt) throws SQLException {
    for (int fieldIndex = 0; fieldIndex < columnTypes.size(); fieldIndex++) {
      ColumnType columnType = columnTypes.get(fieldIndex);
      Schema.Field field = record.getSchema().getField(columnType.getName());
      writeToDB(stmt, field, fieldIndex);
    }
  }

  protected void updateOperation(PreparedStatement stmt) throws SQLException {
    List<String> updatedKeyList = Arrays.asList(relationTableKey.split(","));
    for (int fieldIndex = 0; fieldIndex < columnTypes.size(); fieldIndex++) {
      ColumnType columnType = columnTypes.get(fieldIndex);
      Schema.Field field = record.getSchema().getField(columnType.getName());
      writeToDB(stmt, field, fieldIndex);
      if (fillUpdateParams(updatedKeyList, columnType)) {
        modifiableColumnTypes.add(columnType);
      }
    }

    // Used for filling the question marks for update
    if (operationName != null && relationTableKey != null) {
      for (int fieldIndex = columnTypes.size(); fieldIndex < modifiableColumnTypes.size(); fieldIndex++) {
        ColumnType columnType = modifiableColumnTypes.get(fieldIndex);
        Schema.Field field = record.getSchema().getField(columnType.getName());
        writeToDB(stmt, field, fieldIndex);
      }
    }
  }

  /**
   * Upsert is different for all plugins. So, will be overriding this method to write to query.
   * @param stmt
   * @throws SQLException
   */
  protected void upsertOperation(PreparedStatement stmt) throws SQLException {
    throw new UnsupportedOperationException();
  }

  private boolean fillUpdateParams(List<String> updatedKeyList, ColumnType columnType) {
    if (operationName.equals(Operation.UPDATE) && updatedKeyList.contains(columnType.getName())) {
      return true;
    }
    return false;
  }

  private Schema getNonNullableSchema(Schema.Field field) {
    Schema schema = field.getSchema();
    if (field.getSchema().isNullable()) {
      schema = field.getSchema().getNonNullable();
    }
    Preconditions.checkArgument(schema.getType().isSimpleType(),
                                "Only simple types are supported (boolean, int, long, float, double, string, bytes) " +
                                  "for writing a DBRecord, but found '%s' as the type for column '%s'. Please " +
                                  "remove this column or transform it to a simple type.", schema.getType(),
                                field.getName());
    return schema;
  }

  private void writeToDataOut(DataOutput out, Schema.Field field) throws IOException {
    Schema fieldSchema = getNonNullableSchema(field);
    Schema.Type fieldType = fieldSchema.getType();
    Object fieldValue = record.get(field.getName());

    if (fieldValue == null) {
      return;
    }

    switch (fieldType) {
      case NULL:
        break;
      case STRING:
        // write string appropriately
        out.writeUTF((String) fieldValue);
        break;
      case BOOLEAN:
        out.writeBoolean((Boolean) fieldValue);
        break;
      case INT:
        // write short or int appropriately
        out.writeInt((Integer) fieldValue);
        break;
      case LONG:
        // write date, timestamp or long appropriately
        out.writeLong((Long) fieldValue);
        break;
      case FLOAT:
        // both real and float are set with the same method on prepared statement
        out.writeFloat((Float) fieldValue);
        break;
      case DOUBLE:
        out.writeDouble((Double) fieldValue);
        break;
      case BYTES:
        out.write((byte[]) fieldValue);
        break;
      default:
        throw new IOException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }

  protected void writeToDB(PreparedStatement stmt, @Nullable Schema.Field field, int fieldIndex) throws SQLException {
    if (shouldWriteNullField(field)) {
      writeNullToDB(stmt, fieldIndex);
    } else {
      Schema nonNullableSchema = getNonNullableSchema(field);
      writeNonNullToDB(stmt, nonNullableSchema, field.getName(), fieldIndex);
    }
  }

  /**
   * This method returns true in case a field can support writeNullToDB for the current field.
   * By default, this method returns true when field or field value is set to null.
   *
   * @param field Field
   * @return true if null value of the field can be written to DB
   */
  protected boolean shouldWriteNullField(Schema.Field field) {
    return (field == null || record.get(field.getName()) == null);
  }

  /**
   * This method handle the null field and null field values, by internally using the PreparedStatement.setNull
   * method. Any class requiring a custom handling to write null value for any type should override this method.
   *
   * @param stmt       PreparedStatement object for writing to db
   * @param fieldIndex Field index in the columnTypes
   * @throws SQLException Exception while calling PreparedStatement.setNull
   */
  protected void writeNullToDB(PreparedStatement stmt, int fieldIndex) throws SQLException {
    int sqlIndex = fieldIndex + 1;
    int sqlType = modifiableColumnTypes.get(fieldIndex).getType();
    stmt.setNull(sqlIndex, sqlType);
  }

  /**
   * Write Non null values to DB.
   *
   * @param stmt          PreparedStatement object for writing to db
   * @param fieldSchema   Non-Nullable schema of the field
   * @param fieldName     Current Field from record's schema
   * @param fieldIndex    Field index in the columnTypes
   * @throws SQLException Exception while calling PreparedStatement.set... calls
   */
  protected void writeNonNullToDB(PreparedStatement stmt, Schema fieldSchema,
                                  String fieldName, int fieldIndex) throws SQLException {

    int sqlIndex = fieldIndex + 1;
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();

    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DATE:
          stmt.setDate(sqlIndex, Date.valueOf(record.getDate(fieldName)));
          break;
        case TIME_MILLIS:
        case TIME_MICROS:
          stmt.setTime(sqlIndex, Time.valueOf(record.getTime(fieldName)));
          break;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          stmt.setTimestamp(sqlIndex, Timestamp.from(record.getTimestamp(fieldName).toInstant()));
          break;
        case DECIMAL:
          stmt.setBigDecimal(sqlIndex, record.getDecimal(fieldName));
          break;
        case DATETIME:
          stmt.setTimestamp(sqlIndex, Timestamp.valueOf(record.getDateTime(fieldName)));
          break;
      }
      return;
    }

    Schema.Type fieldType = fieldSchema.getType();
    Object fieldValue = record.get(fieldName);
    switch (fieldType) {
      case NULL:
        stmt.setNull(sqlIndex, modifiableColumnTypes.get(fieldIndex).getType());
        break;
      case STRING:
        // clob can also be written to as setString
        stmt.setString(sqlIndex, (String) fieldValue);
        break;
      case BOOLEAN:
        stmt.setBoolean(sqlIndex, (Boolean) fieldValue);
        break;
      case INT:
        stmt.setInt(sqlIndex, (Integer) fieldValue);
        break;
      case LONG:
        long fieldValueLong = ((Number) fieldValue).longValue();
        stmt.setLong(sqlIndex, fieldValueLong);
        break;
      case FLOAT:
        // both real and float are set with the same method on prepared statement
        stmt.setFloat(sqlIndex, (Float) fieldValue);
        break;
      case DOUBLE:
        stmt.setDouble(sqlIndex, (Double) fieldValue);
        break;
      case BYTES:
        writeBytes(stmt, fieldIndex, sqlIndex, fieldValue);
        break;
      default:
        throw new SQLException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }

  protected void writeBytes(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue)
    throws SQLException {
    byte[] byteValue = fieldValue instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) fieldValue) : (byte[]) fieldValue;
    int parameterType = modifiableColumnTypes.get(fieldIndex).getType();
    if (Types.BLOB == parameterType) {
      stmt.setBlob(sqlIndex, new SerialBlob(byteValue));
      return;
    }
    // handles BINARY, VARBINARY and LOGVARBINARY
    stmt.setBytes(sqlIndex, byteValue);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}

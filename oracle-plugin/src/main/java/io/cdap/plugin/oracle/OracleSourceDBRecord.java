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

import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Oracle Source implementation {@link org.apache.hadoop.mapreduce.lib.db.DBWritable} and
 * {@link org.apache.hadoop.io.Writable}.
 */
public class OracleSourceDBRecord extends DBRecord {

  public OracleSourceDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public OracleSourceDBRecord() {
    // Required by Hadoop DBRecordReader to create an instance
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSourceSchemaReader();
  }

  /**
   * Builds the {@link #record} using the specified {@link ResultSet} for Oracle DB
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    Schema schema = getSchema();
    ResultSetMetaData metadata = resultSet.getMetaData();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);

    // All LONG or LONG RAW columns have to be retrieved from the ResultSet prior to all the other columns.
    // Otherwise, we will face java.sql.SQLException: Stream has already been closed
    for (Schema.Field field : schema.getFields()) {
      // Index of a field in the schema may not be same in the ResultSet,
      // hence find the field by name in the given resultSet
      int columnIndex = resultSet.findColumn(field.getName());
      if (isLongOrLongRaw(metadata.getColumnType(columnIndex))) {
        readField(columnIndex, metadata, resultSet, field, recordBuilder);
      }
    }

    // Read fields of other types
    for (Schema.Field field : schema.getFields()) {
      // Index of a field in the schema may not be same in the ResultSet,
      // hence find the field by name in the given resultSet
      int columnIndex = resultSet.findColumn(field.getName());
      if (!isLongOrLongRaw(metadata.getColumnType(columnIndex))) {
        readField(columnIndex, metadata, resultSet, field, recordBuilder);
      }
    }

    record = recordBuilder.build();
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (OracleSourceSchemaReader.ORACLE_TYPES.contains(sqlType) || sqlType == Types.NCLOB) {
      handleOracleSpecificType(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  @Override
  protected void writeNonNullToDB(PreparedStatement stmt, Schema fieldSchema,
                                  String fieldName, int fieldIndex) throws SQLException {
    int sqlType = columnTypes.get(fieldIndex).getType();
    int sqlIndex = fieldIndex + 1;

    // TIMESTAMP and TIMESTAMPTZ types needs to be handled using the specific oracle types to ensure that the data
    // inserted matches with the provided value. As Oracle driver internally alters the values provided
    // based on session TIME_ZONE parameter which is set implicitly by the Oracle Driver using the
    // JVM's System time zone on the client machine.
    // More details here : https://docs.oracle.com/cd/E13222_01/wls/docs91/jdbc_drivers/oracle.html
    // Handle the case when TimestampTZ type is set to CDAP String type or Timestamp type
    if (sqlType == OracleSourceSchemaReader.TIMESTAMP_TZ) {
      if (Schema.Type.STRING.equals(fieldSchema.getType())) {
        // Deprecated: Handle the case when the TimestampTZ is mapped to CDAP String type
        String timestampString = record.get(fieldName);
        Object timestampTZ = createOracleTimestampWithTimeZone(stmt.getConnection(), timestampString);
        stmt.setObject(sqlIndex, timestampTZ);
      } else {
        // Handle the case when the TimestampTZ is mapped to CDAP Timestamp type
        ZonedDateTime timestamp = record.getTimestamp(fieldName);
        String timestampString = Timestamp.valueOf(timestamp.toOffsetDateTime()
                .atZoneSameInstant(OffsetDateTime.now().getOffset()).toLocalDateTime()).toString();
        Object timestampWithTimeZone = createOracleTimestampWithTimeZone(stmt.getConnection(), timestampString);
        stmt.setObject(sqlIndex, timestampWithTimeZone);
      }
    } else if (sqlType == OracleSourceSchemaReader.TIMESTAMP_LTZ) {
      if (Schema.LogicalType.TIMESTAMP_MICROS.equals(fieldSchema.getLogicalType())) {
        // Deprecated: Handle the case when the TimestampLTZ is mapped to CDAP Timestamp type
        ZonedDateTime timestamp = record.getTimestamp(fieldName);
        String timestampString = Timestamp.valueOf(timestamp.toLocalDateTime()).toString();
        Object timestampWithTimeZone = createOracleTimestampWithLocalTimeZone(stmt.getConnection(), timestampString);
        stmt.setObject(sqlIndex, timestampWithTimeZone);
      } else if (Schema.LogicalType.DATETIME.equals(fieldSchema.getLogicalType())) {
        // Handle the case when the TimestampLTZ is mapped to CDAP Datetime type
        LocalDateTime localDateTime = record.getDateTime(fieldName);
        String timestampString = Timestamp.valueOf(localDateTime).toString();
        Object timestampWithTimeZone = createOracleTimestampWithLocalTimeZone(stmt.getConnection(), timestampString);
        stmt.setObject(sqlIndex, timestampWithTimeZone);
      }
    } else if (sqlType == Types.TIMESTAMP) {
      if (Schema.LogicalType.DATETIME.equals(fieldSchema.getLogicalType())) {
        // Handle the case when Timestamp is mapped to CDAP Datetime type.
        LocalDateTime localDateTime = record.getDateTime(fieldName);
        String timestampString = Timestamp.valueOf(localDateTime).toString();
        Object timestampWithTimeZone = createOracleTimestamp(stmt.getConnection(), timestampString);
        stmt.setObject(sqlIndex, timestampWithTimeZone);
      } else if (Schema.LogicalType.TIMESTAMP_MICROS.equals(fieldSchema.getLogicalType())) {
        // Deprecated: Handle the case when the Timestamp is mapped to CDAP Timestamp type
        super.writeNonNullToDB(stmt, fieldSchema, fieldName, fieldIndex);
      }
    } else {
      super.writeNonNullToDB(stmt, fieldSchema, fieldName, fieldIndex);
    }
  }

  /**
   * Creates an instance of 'oracle.sql.TIMESTAMPTZ' which corresponds to the specified timestamp with time zone string.
   * @param connection sql connection.
   * @param timestampString timestamp with time zone string, such as "2019-07-15 15:57:46.65".
   * @return instance of 'oracle.sql.TIMESTAMPTZ' which corresponds to the specified timestamp with time zone string.
   */
  private Object createOracleTimestampWithTimeZone(Connection connection, String timestampString) {
    try {
      ClassLoader classLoader = connection.getClass().getClassLoader();
      Class<?> timestampTZClass = classLoader.loadClass("oracle.sql.TIMESTAMPTZ");
      return timestampTZClass.getConstructor(Connection.class, String.class).newInstance(connection, timestampString);
    } catch (ClassNotFoundException e) {
      throw new InvalidStageException("Unable to load 'oracle.sql.TIMESTAMPTZ'.", e);
    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new InvalidStageException("Unable to instantiate 'oracle.sql.TIMESTAMPTZ'.", e);
    }
  }

  /**
   * Creates an instance of 'oracle.sql.TIMESTAMPLTZ' which corresponds to the specified timestamp with local time zone.
   * @param connection sql connection.
   * @param timestampString timestamp with local time zone string, such as "2019-07-15 15:57:46.65".
   * @return instance of 'oracle.sql.TIMESTAMPLTZ' which corresponds to the specified timestamp with local time zone
   * string.
   */
  private Object createOracleTimestampWithLocalTimeZone(Connection connection, String timestampString) {
    try {
      ClassLoader classLoader = connection.getClass().getClassLoader();
      Class<?> timestampLTZClass = classLoader.loadClass("oracle.sql.TIMESTAMPLTZ");
      return timestampLTZClass.getConstructor(Connection.class, String.class).newInstance(connection, timestampString);
    } catch (ClassNotFoundException e) {
      throw new InvalidStageException("Unable to load 'oracle.sql.TIMESTAMPLTZ'.", e);
    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new InvalidStageException("Unable to instantiate 'oracle.sql.TIMESTAMPLTZ'.", e);
    }
  }

  /**
   * Creates an instance of 'oracle.sql.TIMESTAMP' which corresponds to the specified timestamp.
   * @param connection sql connection.
   * @param timestampString timestamp string, such as "2019-07-15 15:57:46.65".
   * @return instance of 'oracle.sql.TIMESTAMP' which corresponds to the specified timestamp without timezone string.
   */
  private Object createOracleTimestamp(Connection connection, String timestampString) {
    try {
      ClassLoader classLoader = connection.getClass().getClassLoader();
      Class<?> timestampClass = classLoader.loadClass("oracle.sql.TIMESTAMP");
      return timestampClass.getConstructor(String.class).newInstance(timestampString);
    } catch (ClassNotFoundException e) {
      throw new InvalidStageException("Unable to load 'oracle.sql.TIMESTAMP'.", e);
    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new InvalidStageException("Unable to instantiate 'oracle.sql.TIMESTAMP'.", e);
    }
  }

  /**
   * Retrieves the contents of the BFILE.
   * @param resultSet sql result set.
   * @param columnName BFILE column name.
   * @return bytes contents of the BFILE.
   */
  private byte[] getBfileBytes(ResultSet resultSet, String columnName) throws SQLException {
    Object bfile = resultSet.getObject(columnName);
    if (bfile == null) {
      return null;
    }
    try {
      ClassLoader classLoader = resultSet.getClass().getClassLoader();
      Class<?> oracleBfileClass = classLoader.loadClass("oracle.jdbc.OracleBfile");
      boolean isFileExist = (boolean) oracleBfileClass.getMethod("fileExists").invoke(bfile);
      if (!isFileExist) {
        return null;
      }

      oracleBfileClass.getMethod("openFile").invoke(bfile);
      InputStream binaryStream = (InputStream) oracleBfileClass.getMethod("getBinaryStream").invoke(bfile);
      byte[] bytes = ByteStreams.toByteArray(binaryStream);
      oracleBfileClass.getMethod("closeFile").invoke(bfile);
      return bytes;
    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new InvalidStageException(String.format("Column '%s' is of type 'BFILE', which is not supported with " +
                                                      "this version of the JDBC driver.", columnName), e);
    } catch (IOException e) {
      throw new InvalidStageException(String.format("Error reading the contents of the BFILE at column '%s'.",
                                                    columnName), e);
    }
  }

  private void handleOracleSpecificType(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                                        int columnIndex, int sqlType, int precision, int scale)
    throws SQLException {
    Schema nonNullSchema = field.getSchema().isNullable() ?
            field.getSchema().getNonNullable() : field.getSchema();
    switch (sqlType) {
      case OracleSourceSchemaReader.INTERVAL_YM:
      case OracleSourceSchemaReader.INTERVAL_DS:
      case OracleSourceSchemaReader.LONG:
      case Types.NCLOB:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
      case OracleSourceSchemaReader.TIMESTAMP_TZ:
        if (Schema.Type.STRING.equals(nonNullSchema.getType())) {
          recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        } else {
          // In case of TimestampTZ datatype the getTimestamp(index, Calendar) method call does not
          // return a correct value for any year which is less than the gregorian cutover date. In more details,
          // for data '0001-01-01 01:00:00.000 -08:00' in the Oracle TIMESTAMPTZ field,
          // super.setField sets this '0000-12-31 09:00:00.000Z' in the recordBuilder which is incorrect and the
          // correct value should be '0001-01-01 09:00:00.000Z'.
          Object timeStampObj = resultSet.getObject(columnIndex);
          if (timeStampObj != null) {
            try {
              ClassLoader classLoader = resultSet.getClass().getClassLoader();
              String className = "oracle.sql.TIMESTAMPTZ";
              Class<?> timestampTZClass = classLoader.loadClass(className);
              OffsetDateTime offsetDateTime = (OffsetDateTime) timestampTZClass.getMethod("offsetDateTimeValue",
                      Connection.class).invoke(timeStampObj, resultSet.getStatement().getConnection());
              recordBuilder.setTimestamp(field.getName(), offsetDateTime.atZoneSameInstant(ZoneId.of("UTC")));
            } catch (ClassNotFoundException | NoSuchMethodException
                     | IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        }

        break;
      case Types.TIMESTAMP:
        // Since Oracle Timestamp type does not have any timezone information, it should be converted into the
        // CDAP Datetime type.
        if (Schema.LogicalType.DATETIME.equals(nonNullSchema.getLogicalType())) {
          Timestamp timestamp = resultSet.getTimestamp(columnIndex);
          if (timestamp != null) {
            recordBuilder.setDateTime(field.getName(), timestamp.toLocalDateTime());
          }
        } else {
          // Deprecated: Converting Oracle TIMESTAMP type to CDAP Timestamp type for backward compatibility.
          setField(resultSet, recordBuilder, field, columnIndex, sqlType, precision, scale);
        }
        break;
      case OracleSourceSchemaReader.TIMESTAMP_LTZ:
        // In case of TimestampLTZ datatype the getTimestamp(index, Calendar) method call does not
        // return a correct value for any year which is less than the gregorian cutover date. In more details,
        // for data '0001-01-01 01:00:00.000 -08:00' in the Oracle TIMESTAMPTZ field,
        // super.setField sets this '0000-12-31 09:00:00.000Z[UTC]' in the recordBuilder which is incorrect and the
        // correct value should be '0001-01-01 09:00:00.000Z[UTC]'.
        Object timeStampObj = resultSet.getObject(columnIndex);
        if (Schema.LogicalType.DATETIME.equals(nonNullSchema.getLogicalType())) {
          Timestamp timestampLTZ = resultSet.getTimestamp(columnIndex);
          if (timestampLTZ != null) {
            recordBuilder.setDateTime(field.getName(),
                    OffsetDateTime.of(timestampLTZ.toLocalDateTime(),
                            ZonedDateTime.now().getOffset()).toLocalDateTime());
          }
        } else {
          Timestamp timestamp = resultSet.getTimestamp(columnIndex);
          recordBuilder.setTimestamp(field.getName(), (timestamp != null) ?
                  timestamp.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)) : null);
        }
        break;
      case OracleSourceSchemaReader.BINARY_FLOAT:
        recordBuilder.set(field.getName(), resultSet.getFloat(columnIndex));
        break;
      case OracleSourceSchemaReader.BINARY_DOUBLE:
        recordBuilder.set(field.getName(), resultSet.getDouble(columnIndex));
        break;
      case OracleSourceSchemaReader.BFILE:
        String columnName = resultSet.getMetaData().getColumnName(columnIndex);
        recordBuilder.set(field.getName(), getBfileBytes(resultSet, columnName));
        break;
      case OracleSourceSchemaReader.LONG_RAW:
        recordBuilder.set(field.getName(), resultSet.getBytes(columnIndex));
        break;
      case Types.DECIMAL:
      case Types.NUMERIC:
        // This is the only way to differentiate FLOAT/REAL columns from other numeric columns, that based on NUMBER.
        // Since FLOAT is a subtype of the NUMBER data type, 'getColumnType' and 'getColumnTypeName' can not be used.
        if (Double.class.getTypeName().equals(resultSet.getMetaData().getColumnClassName(columnIndex))) {
          recordBuilder.set(field.getName(), resultSet.getDouble(columnIndex));
        } else {
          if (precision == 0) {
            Schema nonNullableSchema = field.getSchema().isNullable() ?
                                        field.getSchema().getNonNullable() : field.getSchema();
            if (Schema.LogicalType.DECIMAL.equals(nonNullableSchema.getLogicalType())) {
              // Handle the field using the schema set in the output schema
              BigDecimal decimal = resultSet.getBigDecimal(columnIndex, getScale(field.getSchema()));
              recordBuilder.setDecimal(field.getName(), decimal);
            } else {
              // In case of Number defined without precision and scale convert to String type
              recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
            }
          } else {
            // It's required to pass 'scale' parameter since in the case of Oracle, scale of 'BigDecimal' depends on the
            // scale set in the logical schema. For example for value '77.12' if the scale set in the logical schema is
            // set to 4 then the number will change to '77.1200'. Also if the value is '22.1274' and the logical schema
            // scale is set to 2 then the decimal value used will be '22.13' after rounding.
            BigDecimal decimal = resultSet.getBigDecimal(columnIndex, getScale(field.getSchema()));
            recordBuilder.setDecimal(field.getName(), decimal);
          }
        }
    }
  }

  /**
   * Get the scale set in Non-nullable schema associated with the schema
   * */
  private int getScale(Schema schema) {
    return schema.isNullable() ? schema.getNonNullable().getScale() : schema.getScale();
  }

  private boolean isLongOrLongRaw(int columnType) {
    return columnType == OracleSourceSchemaReader.LONG || columnType == OracleSourceSchemaReader.LONG_RAW;
  }

  private void readField(int columnIndex, ResultSetMetaData metadata, ResultSet resultSet, Schema.Field field,
                         StructuredRecord.Builder recordBuilder) throws SQLException {
    int sqlType = metadata.getColumnType(columnIndex);
    int sqlPrecision = metadata.getPrecision(columnIndex);
    int sqlScale = metadata.getScale(columnIndex);

    handleField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
  }

  @Override
  protected void writeBytes(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue)
    throws SQLException {
    byte[] byteValue = fieldValue instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) fieldValue) : (byte[]) fieldValue;
    // handles BINARY, VARBINARY and LOGVARBINARY
    stmt.setBytes(sqlIndex, byteValue);
  }
}

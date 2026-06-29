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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

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
   * Maps Oracle string data type inside UDT to their corresponding java.sql.Types integer constants
   */
  private static final Map<String, Integer> DATA_TYPE_MAP = new HashMap<>();
  static {
    DATA_TYPE_MAP.put("TIMESTAMP WITH LOCAL TZ", TIMESTAMP_LTZ);
    DATA_TYPE_MAP.put("TIMESTAMP WITH TZ", TIMESTAMP_TZ);
    DATA_TYPE_MAP.put("TIMESTAMP", Types.TIMESTAMP);
    DATA_TYPE_MAP.put("DATE", Types.TIMESTAMP);
    DATA_TYPE_MAP.put("FLOAT", Types.DOUBLE);
    DATA_TYPE_MAP.put("BINARY_FLOAT", BINARY_FLOAT);
    DATA_TYPE_MAP.put("REAL", Types.DOUBLE);
    DATA_TYPE_MAP.put("BINARY_DOUBLE", BINARY_DOUBLE);
    DATA_TYPE_MAP.put("DOUBLE", Types.DOUBLE);
    DATA_TYPE_MAP.put("BFILE", BFILE);
    DATA_TYPE_MAP.put("RAW", LONG_RAW);
    DATA_TYPE_MAP.put("LONG RAW", LONG_RAW);
    DATA_TYPE_MAP.put("LONG", LONG);
    DATA_TYPE_MAP.put("INTERVAL DAY TO SECOND", INTERVAL_DS);
    DATA_TYPE_MAP.put("INTERVAL YEAR TO MONTH", INTERVAL_YM);
    DATA_TYPE_MAP.put("XMLTYPE", Types.SQLXML);
    DATA_TYPE_MAP.put("ARRAY", Types.ARRAY);
    DATA_TYPE_MAP.put("ANYDATA", Types.JAVA_OBJECT);
    DATA_TYPE_MAP.put("OTHER", Types.OTHER);
    DATA_TYPE_MAP.put("NUMBER", Types.NUMERIC);
    DATA_TYPE_MAP.put("DECIMAL", Types.DECIMAL);
    DATA_TYPE_MAP.put("INTEGER", Types.INTEGER);
    DATA_TYPE_MAP.put("ROWID", Types.ROWID);
    DATA_TYPE_MAP.put("UROWID", Types.ROWID);
    DATA_TYPE_MAP.put("BLOB", Types.BLOB);
    DATA_TYPE_MAP.put("CLOB", Types.CLOB);
    DATA_TYPE_MAP.put("NCLOB", Types.NCLOB);
    DATA_TYPE_MAP.put("VARCHAR2", Types.VARCHAR);
    DATA_TYPE_MAP.put("VARCHAR", Types.VARCHAR);
    DATA_TYPE_MAP.put("CHAR", Types.CHAR);
    DATA_TYPE_MAP.put("CHAR2", Types.CHAR);
    DATA_TYPE_MAP.put("NCHAR", Types.NCHAR);
    DATA_TYPE_MAP.put("NVARCHAR2", Types.NVARCHAR);
  }

  /**
   * Logger instance for Oracle Schema reader.
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleSourceSchemaReader.class);

  public static final Set<Integer> ORACLE_TYPES = ImmutableSet.of(
    INTERVAL_DS,
    INTERVAL_YM,
    Types.TIMESTAMP,
    TIMESTAMP_TZ,
    TIMESTAMP_LTZ,
    BINARY_FLOAT,
    BINARY_DOUBLE,
    BFILE,
    LONG,
    Types.SQLXML,
    LONG_RAW,
    Types.NUMERIC,
    Types.DECIMAL
  );

  private final String sessionID;
  private final Boolean isTimestampOldBehavior;
  private final Boolean isPrecisionlessNumAsDecimal;
  private final Boolean isTimestampLtzFieldTimestamp;
  private final Boolean isXmlTypeEnabled;
  private Connection connection;

  public OracleSourceSchemaReader() {
    this(null, false, false, false, false);
  }
  public OracleSourceSchemaReader(@Nullable String sessionID, boolean isTimestampOldBehavior,
                                  boolean isPrecisionlessNumAsDecimal, boolean isTimestampLtzFieldTimestamp,
                                  boolean isXmlTypeEnabled) {
    this.sessionID = sessionID;
    this.isTimestampOldBehavior = isTimestampOldBehavior;
    this.isPrecisionlessNumAsDecimal = isPrecisionlessNumAsDecimal;
    this.isTimestampLtzFieldTimestamp = isTimestampLtzFieldTimestamp;
    this.isXmlTypeEnabled = isXmlTypeEnabled;
  }

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int sqlType = metadata.getColumnType(index);
    String owner = (metadata.getColumnTypeName(index) != null
            && metadata.getColumnTypeName(index).contains(".")) ? metadata.getColumnTypeName(index)
            .substring(0, metadata.getColumnTypeName(index).lastIndexOf('.')) : null;

    return getSchemaMapping(sqlType, metadata.getColumnClassName(index), metadata.getPrecision(index),
            metadata.getScale(index), metadata.getColumnName(index), metadata.getColumnTypeName(index),
            metadata.isSigned(index), owner, 0);
  }

  public Schema getSchemaMapping(int sqlType, String columnClassName, int columnPrecision,
                                 int columnScale, String columnName, String columnTypeName,
                                 boolean isSigned, String owner, int nestingLevel) throws SQLException {

    switch (sqlType) {
      case TIMESTAMP_TZ:
        return isTimestampOldBehavior ? Schema.of(Schema.Type.STRING) : Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case TIMESTAMP_LTZ:
        return getTimestampLtzSchema();
      case Types.TIMESTAMP:
        return isTimestampOldBehavior ? super.getSchema(columnTypeName, sqlType,
                columnPrecision, columnScale, columnName, isSigned) : Schema.of(Schema.LogicalType.DATETIME);
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
      case Types.SQLXML:
        // Enabling XML type support for DTS connectors only as it is not in working state in CDAP plugin.
        return isXmlTypeEnabled ? Schema.of(Schema.Type.STRING) : super.getSchema(columnTypeName,
                sqlType, columnPrecision, columnScale, columnName, isSigned);
      case Types.NUMERIC:
      case Types.DECIMAL:
        // FLOAT and REAL are returned as java.sql.Types.NUMERIC but with value that is a java.lang.Double
        if (Double.class.getTypeName().equals(columnClassName)) {
          return Schema.of(Schema.Type.DOUBLE);
        } else {
          int precision = columnPrecision; // total number of digits
          int scale = columnScale; // digits after the decimal point
          // For a Number type without specified precision and scale, precision will be 0 and scale will be -127
          if (precision == 0) {
            // reference : https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
            if (isPrecisionlessNumAsDecimal) {
              precision = 38;
              scale = 0;
              LOG.warn(String.format("%s type with undefined precision and scale is detected, "
                                       + "there may be a precision loss while running the pipeline. "
                                       + "Please define an output precision and scale for field '%s' to avoid "
                                       + "precision loss.",
                                     columnTypeName, columnName));
              return Schema.decimalOf(precision, scale);
            } else {
              LOG.warn(String.format("Field '%s' is a %s type without precision and scale, "
                                       + "converting into STRING type to avoid any precision loss.",
                                     columnName, columnTypeName, columnName));
              return Schema.of(Schema.Type.STRING);
            }
          }
          return Schema.decimalOf(precision, scale);
        }
      case Types.STRUCT:
        if (connection == null) {
          throw new SQLException("Cannot resolve STRUCT schema without a database connection. "
                  + "Use getSchemaFields(ResultSet) to enable STRUCT type resolution.");
        }
        if (nestingLevel >= 4) {
          throw new IllegalArgumentException(String.format("Cannot resolve STRUCT schema for attribute %s with " +
                  "nested structure depth more than 4.", columnName));
        }
        return getStructSchema(connection, columnTypeName, owner, nestingLevel);
      default:
        return super.getSchema(columnTypeName, sqlType, columnPrecision, columnScale, columnName, isSigned);
    }
  }

  @Override
  public List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    this.connection = resultSet.getStatement().getConnection();
    return super.getSchemaFields(resultSet);
  }

  /**
   * Builds a CDAP RECORD schema for an Oracle STRUCT type by querying the
   * database metadata
   * for the type's attributes.
   *
   * @param connection the database connection
   * @param typeName   the Oracle type name (e.g., "ADDRESS_TYPE")
   * @param owner   the Owner of the user-defined data type
   * @param level the level of nesting of the user-defined data type
   * @return a CDAP RECORD schema with fields corresponding to the STRUCT's
   *         attributes
   */
  private Schema getStructSchema(Connection connection, String typeName, String owner, int level) throws SQLException {
    List<Schema.Field> fields = new ArrayList<>();
    String sql = "SELECT * FROM ALL_TYPE_ATTRS WHERE TYPE_NAME = ? AND OWNER = ? ORDER BY ATTR_NO";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, typeName.substring(typeName.lastIndexOf('.') + 1));
      stmt.setString(2, owner);

      try (ResultSet attrRs = stmt.executeQuery()) {
        while (attrRs.next()) {
          String attrName = attrRs.getString("ATTR_NAME");
          String attrTypeName = attrRs.getString("ATTR_TYPE_NAME");
          int attrSize = attrRs.getInt("PRECISION");
          int attrScale = attrRs.getInt("SCALE");
          Integer sqlType = DATA_TYPE_MAP.getOrDefault(attrTypeName, null);

          int nextLevel = level;
          if (sqlType == null) {
            owner = attrRs.getString("ATTR_TYPE_OWNER");
            if (owner == null || owner.isEmpty()) {
              throw new SQLException(String.format("Attribute '%s' is not a primitive type, but it lacks a type " +
                      "owner. Therefore, it cannot be resolved as a STRUCT type. ", attrName));
            }
            sqlType = Types.STRUCT;
            nextLevel = level + 1;
          }
          Schema attrSchema = getSchemaMapping(sqlType, null, attrSize,
                  attrScale, attrName, attrTypeName, true, owner, nextLevel);
          fields.add(Schema.Field.of(attrName, attrSchema));
        }
      }
    }

    if (fields.isEmpty()) {
      throw new SQLException(String.format(
          "No attributes found for Oracle STRUCT type '%s'. "
              + "Ensure the type exists and is accessible.",
          typeName));
    }

    return Schema.recordOf(typeName, fields);
  }

  private Schema getTimestampLtzSchema() {
    return isTimestampOldBehavior || isTimestampLtzFieldTimestamp
      ? Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)
      : Schema.of(Schema.LogicalType.DATETIME);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    if (sessionID == null) {
      return false;
    }
    return metadata.getColumnName(index).equals("c_" + sessionID) ||
      metadata.getColumnName(index).equals("s_" + sessionID);
  }
}

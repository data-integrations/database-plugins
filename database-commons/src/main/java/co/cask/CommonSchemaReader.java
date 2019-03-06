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

package co.cask;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import javax.annotation.Nullable;


/**
 * Common schema reader for mapping non specific DB types.
 */
public class CommonSchemaReader implements SchemaReader {

  protected final ResultSet resultSet;

  public CommonSchemaReader(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public List<Schema.Field> getSchemaFields(@Nullable String schemaStr)
    throws SQLException {
    Schema resultsetSchema = Schema.recordOf("resultset", getSchemaFields());
    Schema schema;

    if (!Strings.isNullOrEmpty(schemaStr)) {
      try {
        schema = Schema.parseJson(schemaStr);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema string %s", schemaStr), e);
      }
      for (Schema.Field field : schema.getFields()) {
        Schema.Field resultsetField = resultsetSchema.getField(field.getName());
        if (resultsetField == null) {
          throw new IllegalArgumentException(String.format("Schema field %s is not present in input record",
                                                           field.getName()));
        }
        Schema resultsetFieldSchema = resultsetField.getSchema().isNullable() ?
          resultsetField.getSchema().getNonNullable() : resultsetField.getSchema();
        Schema simpleSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();

        if (!resultsetFieldSchema.equals(simpleSchema)) {
          throw new IllegalArgumentException(String.format("Schema field %s has type %s but in input record found " +
                                                             "type %s ",
                                                           field.getName(), simpleSchema.getType(),
                                                           resultsetFieldSchema.getType()));
        }
      }
      return schema.getFields();

    }
    return resultsetSchema.getFields();
  }

  public List<Schema.Field> getSchemaFields() throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
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

  // given a sql type return schema type
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR

    int sqlType = metadata.getColumnType(index);

    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.ROWID:
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        type = Schema.Type.INT;
        break;

      case Types.BIGINT:
        type = Schema.Type.LONG;
        break;

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        int precision = metadata.getPrecision(index); // total number of digits
        int scale = metadata.getScale(index); // digits after the decimal point
        // if there are no digits after the point, use integer types
        type = scale != 0 ? Schema.Type.DOUBLE :
          // with 10 digits we can represent 2^32 and LONG is required
          precision > 9 ? Schema.Type.LONG : Schema.Type.INT;
        break;

      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case Types.TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case Types.TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return Schema.of(type);
  }


}

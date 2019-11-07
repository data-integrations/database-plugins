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

package io.cdap.plugin.neo4j.source;

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.plugin.db.CommonSchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Neo4j schema reader for mapping Neo4j DB types.
 */
public class Neo4jSchemaReader extends CommonSchemaReader {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jSchemaReader.class);

  @Override
  public List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    resultSet.next();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      if (shouldIgnoreColumn(metadata, i)) {
        continue;
      }
      String columnName = metadata.getColumnName(i).replace("}", "").replace(".", "_")
        .replace("(", "_");
      Schema columnSchema = getSchema(resultSet, metadata, i);
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  public Schema getSchema(ResultSet rs, ResultSetMetaData metadata, int index) throws SQLException {
    int sqlType = metadata.getColumnType(index);
    String columnTypeName = metadata.getColumnTypeName(index);
    if ("PATH".equals(columnTypeName)) {
      throw new IllegalArgumentException("Unsupported type 'PATH'");
    }

    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.BOOLEAN:
        type = Schema.Type.BOOLEAN;
        break;
      case Types.INTEGER:
        type = Schema.Type.LONG;
        break;
      case Types.FLOAT:
        type = Schema.Type.DOUBLE;
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        int precision = metadata.getPrecision(index); // total number of digits
        int scale = metadata.getScale(index); // digits after the decimal point
        return Schema.decimalOf(precision, scale);
      case Types.DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case Types.TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case Types.ARRAY:
        List list = rs.getObject(index, List.class);
        Class objClass = list.get(0).getClass();
        return Schema.arrayOf(getSubSchema(objClass));
      case Types.JAVA_OBJECT:
        if (rs.getObject(index) instanceof Map) {
          Map map = rs.getObject(index, Map.class);
          String columnName = metadata.getColumnName(index).replace("}", "")
            .replace(" ", "_")
            .replace(".", "_")
            .replace("(", "_");
          return processMapObject(columnName, map);
        }
        if (rs.getObject(index) instanceof byte[]) {
          type = Schema.Type.BYTES;
        }
        break;
      case Types.BIT:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.BIGINT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.OTHER:
      case Types.REF:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return Schema.of(type);
  }

  private Schema processMapObject(String fieldName, Map mapObject) {
    List<Schema.Field> fields = new ArrayList<>();
    mapObject.keySet().forEach(k -> {
      Object o = mapObject.get(k);
      if (o instanceof List) {
        List l = ((List) mapObject.get(k));
        Class subClass = !l.isEmpty() ? l.get(0).getClass() : String.class;
        fields.add(Schema.Field.of((String) k, Schema.arrayOf(getSubSchema(subClass))));
      } else if (o instanceof Map) {
        fields.add(Schema.Field.of((String) k, processMapObject((String) k, (Map) o)));
      } else {
        fields.add(Schema.Field.of((String) k, getSubSchema(o.getClass())));
      }
    });
    return Schema.recordOf(fieldName, fields);
  }

  private Schema getSubSchema(Class objClass) {
    Schema schema = Schema.of(Schema.Type.STRING);
    if (Long.class.equals(objClass)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (Integer.class.equals(objClass)) {
      return Schema.of(Schema.Type.INT);
    }
    if (Double.class.equals(objClass)) {
      return Schema.of(Schema.Type.DOUBLE);
    }
    if (Boolean.class.equals(objClass)) {
      return Schema.of(Schema.Type.BOOLEAN);
    }
    if (Date.class.equals(objClass)) {
      return Schema.of(Schema.LogicalType.DATE);
    }
    if (Time.class.equals(objClass)) {
      return Schema.of(Schema.LogicalType.TIME_MICROS);
    }
    if (Timestamp.class.equals(objClass)) {
      return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
    }
    if (byte[].class.equals(objClass)) {
      return Schema.of(Schema.Type.BYTES);
    }

    return schema;
  }
}

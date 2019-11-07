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

package io.cdap.plugin.neo4j;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.neo4j.source.Neo4jSchemaReader;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Writable class for Neo4j Source/Sink.
 */
public class Neo4jRecord extends DBRecord {
  private static final Logger LOG = LoggerFactory.getLogger(Neo4jRecord.class);

  private Map<Integer, String> positions;

  public Neo4jRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  public Neo4jRecord() {
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new Neo4jSchemaReader();
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (Types.JAVA_OBJECT == sqlType) {
      handleSpecificType(resultSet, recordBuilder, field, columnIndex);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  private void handleSpecificType(ResultSet resultSet,
                                  StructuredRecord.Builder recordBuilder,
                                  Schema.Field field, int columnIndex) throws SQLException {
    if (resultSet.getObject(columnIndex) instanceof byte[]) {
      recordBuilder.set(field.getName(), resultSet.getObject(columnIndex));
    } else {
      recordBuilder.set(field.getName(), buildRecord(field.getSchema(), resultSet.getObject(columnIndex, Map.class)));
    }
  }

  private StructuredRecord buildRecord(Schema schema, Map values) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (int i = 0; i < schema.getFields().size(); i++) {
      Schema.Field field = schema.getFields().get(i);
      Object o = values.get(field.getName());
      if (o instanceof Date) {
        builder.setDate(field.getName(), ((Date) o).toLocalDate());
      } else if (o instanceof Time) {
        builder.setTime(field.getName(), ((Time) o).toLocalTime());
      } else if (o instanceof Timestamp) {
        Instant instant = ((Timestamp) o).toInstant();
        builder.setTimestamp(field.getName(), instant.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
      } else if (o instanceof BigDecimal) {
        builder.setDecimal(field.getName(), (BigDecimal) o);
      } else if (o instanceof Map) {
        builder.set(field.getName(), buildRecord(field.getSchema(), (Map) o));
      } else {
        builder.set(field.getName(), o);
      }
    }

    return builder.build();
  }

  @Override
  public void write(PreparedStatement stmt) throws SQLException {
    for (int i = 0; i < positions.size(); i++) {
      Map<String, Object> results = new TreeMap<>();
      String fields = positions.get(i + 1);
      List<String> fieldsList = Arrays.stream(fields.split(",")).map(String::trim).collect(Collectors.toList());
      if (fieldsList.size() == 1 && fieldsList.get(0).equals("*")) {
        fieldsList = record.getSchema().getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
      }
      for (String v : fieldsList) {
        Schema.Field field = record.getSchema().getField(v);
        results.put(v, processData(record, field, true));
      }
      stmt.setObject(i + 1, results);
    }
  }

  private Object processData(StructuredRecord record, @Nullable Schema.Field field, boolean baseRecord)
    throws SQLException {
    if (field == null) {
      return null;
    }
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();
    Object fieldValue = record.get(fieldName);

    if (fieldValue == null) {
      return null;
    }

    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DATE:
          return record.getDate(fieldName);
        case TIME_MILLIS:
        case TIME_MICROS:
          return record.getTime(fieldName);
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return record.getTimestamp(fieldName);
        case DECIMAL:
          return record.getDecimal(fieldName);
      }
      return null;
    }

    switch (fieldType) {
      case NULL:
        return null;
      case BYTES:
        return fieldValue instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) fieldValue) : (byte[]) fieldValue;
      case RECORD:
        return processRecord((StructuredRecord) fieldValue, baseRecord);
      case STRING:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case ARRAY:
        return fieldValue;
      default:
        throw new SQLException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }

  private Object processRecord(StructuredRecord record, boolean baseRecord) throws SQLException {
    Schema schema = record.getSchema();

    if (schema.getFields().stream().map(Schema.Field::toString).collect(Collectors.toList())
      .containsAll(Neo4jConstants.DURATION_RECORD_FIELDS.stream().map(Schema.Field::toString)
                     .collect(Collectors.toList()))) {
      return new InternalIsoDuration(record.get("months"), record.get("days"), record.get("seconds"),
                                     record.get("nanoseconds"));
    } else if (schema.getFields().stream().map(Schema.Field::toString).collect(Collectors.toList())
      .containsAll(Neo4jConstants.POINT_3D_RECORD_FIELDS.stream().map(Schema.Field::toString)
                     .collect(Collectors.toList()))) {
      return new InternalPoint3D(record.get("srid"), record.get("x"), record.get("y"), record.get("z"));
    } else if (schema.getFields().stream().map(Schema.Field::toString).collect(Collectors.toList())
      .containsAll(Neo4jConstants.POINT_2D_RECORD_FIELDS.stream().map(Schema.Field::toString)
                     .collect(Collectors.toList()))) {
      return new InternalPoint2D(record.get("srid"), record.get("x"), record.get("y"));
    } else {
      Map<String, Object> resultMap = new TreeMap<>();
      for (Schema.Field field : schema.getFields()) {
        if (Neo4jConstants.NEO4J_SYS_FIELDS.contains(field.getName())) {
          continue;
        }
        if (field.getSchema().getType().equals(Schema.Type.RECORD)) {
          if (!baseRecord) {
            throw new SQLException(String.format("Unsupported datatype: %s with value: %s.",
                                                 field.getSchema().getType(),
                                                 record.get(field.getName())));
          } else {
            resultMap.put(field.getName(), processRecord(record.get(field.getName()), false));
          }
        }
        resultMap.put(field.getName(), processData(record, field, false));
      }
      return resultMap;
    }
  }

  public void setPositions(Map<Integer, String> positions) {
    if (this.positions == null) {
      this.positions = positions;
    }
  }
}

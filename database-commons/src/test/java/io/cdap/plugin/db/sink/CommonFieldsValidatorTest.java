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

package io.cdap.plugin.db.sink;

import com.google.common.collect.ImmutableSet;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;
import java.util.Set;

/**
 * Test class for validation fields mapping.
 */
public class CommonFieldsValidatorTest {
  private static final FieldsValidator VALIDATOR = new CommonFieldsValidator();

  @Test
  public void testIsFieldCompatible() {
    validateFieldCompatible(Schema.Type.INT, Schema.LogicalType.DATE, Types.DATE, true);
    validateFieldCompatible(Schema.Type.INT, Schema.LogicalType.TIME_MILLIS, Types.TIME, true);
    validateFieldCompatible(Schema.Type.LONG, Schema.LogicalType.TIME_MICROS, Types.TIME, true);
    validateFieldCompatible(Schema.Type.LONG, Schema.LogicalType.TIMESTAMP_MILLIS, Types.TIMESTAMP, true);
    validateFieldCompatible(Schema.Type.LONG, Schema.LogicalType.TIMESTAMP_MICROS, Types.TIMESTAMP, true);
    validateFieldCompatible(Schema.Type.BYTES, Schema.LogicalType.DECIMAL, Types.NUMERIC, true);
    validateFieldCompatible(Schema.Type.BYTES, Schema.LogicalType.DECIMAL, Types.DECIMAL, true);
    validateFieldCompatible(Schema.Type.NULL, null, 0, true);
    validateFieldCompatible(Schema.Type.BOOLEAN, null, Types.BOOLEAN, true);
    validateFieldCompatible(Schema.Type.BOOLEAN, null, Types.BIT, true);
    validateFieldCompatible(Schema.Type.INT, null, Types.INTEGER, true);
    validateFieldCompatible(Schema.Type.INT, null, Types.SMALLINT, true);
    validateFieldCompatible(Schema.Type.INT, null, Types.TINYINT, true);
    validateFieldCompatible(Schema.Type.FLOAT, null, Types.REAL, true);
    validateFieldCompatible(Schema.Type.FLOAT, null, Types.FLOAT, true);
    validateFieldCompatible(Schema.Type.DOUBLE, null, Types.DOUBLE, true);
    validateFieldCompatible(Schema.Type.BYTES, null, Types.BINARY, true);
    validateFieldCompatible(Schema.Type.BYTES, null, Types.VARBINARY, true);
    validateFieldCompatible(Schema.Type.BYTES, null, Types.LONGVARBINARY, true);
    validateFieldCompatible(Schema.Type.BYTES, null, Types.BLOB, true);
    validateFieldCompatible(Schema.Type.STRING, null, Types.VARCHAR, true);
    validateFieldCompatible(Schema.Type.STRING, null, Types.CHAR, true);
    validateFieldCompatible(Schema.Type.STRING, null, Types.CLOB, true);
    validateFieldCompatible(Schema.Type.STRING, null, Types.LONGNVARCHAR, true);
    validateFieldCompatible(Schema.Type.STRING, null, Types.LONGVARCHAR, true);
    validateFieldCompatible(Schema.Type.STRING, null, Types.NCHAR, true);
    validateFieldCompatible(Schema.Type.STRING, null, Types.NCLOB, true);
    validateFieldCompatible(Schema.Type.LONG, null, Types.TIMESTAMP, false);
  }

  @Test
  public void testValidateFields() throws Exception {
    Schema schema = Schema.recordOf(
      "dbRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("SCORE", Schema.of(Schema.Type.DOUBLE))
    );

    Set<String> columns = ImmutableSet.of(
      "ID",
      "NAME",
      "SCORE"
    );

    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    resultSetMetaData.setColumnName(1, "ID");
    resultSetMetaData.setColumnTypeName(1, "INTEGER");
    resultSetMetaData.setColumnType(1, 4);

    resultSetMetaData.setColumnName(2, "NAME");
    resultSetMetaData.setColumnTypeName(2, "STRING");
    resultSetMetaData.setColumnType(2, 12);

    resultSetMetaData.setColumnName(3, "SCORE");
    resultSetMetaData.setColumnTypeName(3, "DOUBLE");
    resultSetMetaData.setColumnType(3, 8);

    MockResultSet resultSet = new MockResultSet("data");
    resultSet.addColumns(columns);
    resultSet.setResultSetMetaData(resultSetMetaData);

    MockFailureCollector collector = new MockFailureCollector();
    VALIDATOR.validateFields(schema, resultSet, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateFieldsWithInvalidMapping() throws Exception {
    Schema schema = Schema.recordOf(
      "dbRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("SCORE", Schema.of(Schema.Type.DOUBLE))
    );

    Set<String> columns = ImmutableSet.of(
      "ID",
      "NAME",
      "SCORE"
    );

    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    resultSetMetaData.setColumnName(1, "ID");
    resultSetMetaData.setColumnTypeName(1, "INTEGER");
    resultSetMetaData.setColumnType(1, 4);

    resultSetMetaData.setColumnName(2, "NAME");
    resultSetMetaData.setColumnTypeName(2, "STRING");
    resultSetMetaData.setColumnType(2, 12);

    resultSetMetaData.setColumnName(3, "SCORE");
    resultSetMetaData.setColumnTypeName(3, "STRING");
    resultSetMetaData.setColumnType(3, 12);

    MockResultSet resultSet = new MockResultSet("data");
    resultSet.addColumns(columns);
    resultSet.setResultSetMetaData(resultSetMetaData);

    MockFailureCollector collector = new MockFailureCollector();
    VALIDATOR.validateFields(schema, resultSet, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    String attribute = collector.getValidationFailures().iterator().next().getCauses().get(0)
      .getAttribute(CauseAttributes.INPUT_SCHEMA_FIELD);
    Assert.assertEquals("SCORE", attribute);
  }

  @Test
  public void testValidateFieldsWithNullable() throws Exception {
    Schema schema = Schema.recordOf(
      "dbRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("SCORE", Schema.of(Schema.Type.DOUBLE))
    );

    Set<String> columns = ImmutableSet.of(
      "ID",
      "NAME",
      "SCORE"
    );

    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    resultSetMetaData.setColumnName(1, "ID");
    resultSetMetaData.setColumnTypeName(1, "INTEGER");
    resultSetMetaData.setColumnType(1, 4);

    resultSetMetaData.setColumnName(2, "NAME");
    resultSetMetaData.setColumnTypeName(2, "STRING");
    resultSetMetaData.setColumnType(2, 12);

    MockResultSet resultSet = new MockResultSet("data");
    resultSet.addColumns(columns);
    resultSet.setResultSetMetaData(resultSetMetaData);

    MockFailureCollector collector = new MockFailureCollector();
    VALIDATOR.validateFields(schema, resultSet, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    String attribute = collector.getValidationFailures().iterator().next().getCauses().get(0)
      .getAttribute(CauseAttributes.INPUT_SCHEMA_FIELD);
    Assert.assertEquals("SCORE", attribute);
  }

  public void validateFieldCompatible(Schema.Type fieldType, Schema.LogicalType fieldLogicalType, int sqlType,
                                      boolean isCompatible) {
    String errorMessage = String.format("Expected type '%s' is %s with sql type '%d'",
                                        fieldType,
                                        isCompatible ? "compatible" : "not compatible",
                                        sqlType);
    Assert.assertEquals(errorMessage, isCompatible, VALIDATOR.isFieldCompatible(fieldType, fieldLogicalType, sqlType));
  }
}

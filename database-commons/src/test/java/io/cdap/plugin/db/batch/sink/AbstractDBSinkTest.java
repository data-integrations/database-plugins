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

package io.cdap.plugin.db.batch.sink;

import com.google.common.collect.ImmutableList;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import io.cdap.plugin.db.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test class for abstract sink.
 */
public class AbstractDBSinkTest {

  @Test
  public void testGetMatchedColumnTypeList() throws Exception {
    List<String> columns = ImmutableList.of(
      "ID",
      "NAME",
      "AGE"
    );

    List<ColumnType> expectedColumns = new ArrayList<>();

    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    for (int i = 0; i < columns.size(); i++) {
      resultSetMetaData.setColumnName(i + 1, columns.get(i));
      resultSetMetaData.setColumnTypeName(i + 1, "STRING");
      resultSetMetaData.setColumnType(i + 1, i);
      expectedColumns.add(new ColumnType(columns.get(i), "STRING", i));
    }

    MockResultSet resultSet = new MockResultSet("data");
    Set<String> columnNamesSet = new HashSet<String>();
    columnNamesSet.addAll(columns);
    resultSet.addColumns(columnNamesSet);
    resultSet.setResultSetMetaData(resultSetMetaData);

    List<ColumnType> result = AbstractDBSink.getMatchedColumnTypeList(resultSet, columns);

    Assert.assertEquals(expectedColumns, result);
  }

  @Test
  public void testGetMismatchColumnTypeList() throws Exception {
    List<String> wrongColumns = ImmutableList.of(
      "MY_ID",
      "NAME",
      "SCORE"
    );

    List<String> columns = ImmutableList.of(
      "ID",
      "NAME",
      "SCORE"
    );

    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    for (int i = 0; i < columns.size(); i++) {
      resultSetMetaData.setColumnName(i + 1, columns.get(i));
      resultSetMetaData.setColumnTypeName(i + 1, "STRING");
      resultSetMetaData.setColumnType(i + 1, i);
    }

    MockResultSet resultSet = new MockResultSet("data");
    Set<String> columnNamesSet = new HashSet<String>();
    columnNamesSet.addAll(columns);
    resultSet.addColumns(columnNamesSet);
    resultSet.setResultSetMetaData(resultSetMetaData);

    try {
      AbstractDBSink.getMatchedColumnTypeList(resultSet, wrongColumns);
      Assert.fail(String.format("Expected to throw %s", IllegalArgumentException.class.getName()));
    } catch (IllegalArgumentException e) {
      String errorMessage = "Missing column 'MY_ID' in SQL table";
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  @Test
  public void testDifferentOrderOfFieldsInResultSet() throws Exception {
    List<String> diffOrdCol = ImmutableList.of(
      "Name",
      "SCORE",
      "ID"
    );

    List<String> columns = ImmutableList.of(
      "ID",
      "NAME",
      "SCORE"
    );

    List<String> typeName = ImmutableList.of(
      "INT",
      "STRING",
      "DOUBLE"
    );

    List<Integer> typeValue = ImmutableList.of(
      1,
      2,
      3
    );

    List<ColumnType> expectedColumns = new ArrayList<>();
    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    for (int i = 0; i < columns.size(); i++) {
      resultSetMetaData.setColumnName(i + 1, columns.get(i));
      resultSetMetaData.setColumnTypeName(i + 1, typeName.get(i));
      resultSetMetaData.setColumnType(i + 1, typeValue.get(i));
      expectedColumns.add(new ColumnType(columns.get(i), typeName.get(i), typeValue.get(i)));
    }

    MockResultSet resultSet = new MockResultSet("data");
    Set<String> columnNamesSet = new HashSet<String>();
    columnNamesSet.addAll(columns);
    resultSet.addColumns(columnNamesSet);
    resultSet.setResultSetMetaData(resultSetMetaData);

    List<ColumnType> actualColumns = AbstractDBSink.getMatchedColumnTypeList(resultSet, diffOrdCol);

    // Assert that all expected fields are present in the actual fields
    for (ColumnType exColType : expectedColumns) {
      Assert.assertTrue(actualColumns.contains(exColType));
    }
  }

  @Test
  public void testSubsetColumnsInResultSet() throws Exception {
    List<String> subsetCol = ImmutableList.of(
      "SCORE",
      "ID"
    );

    List<String> columns = ImmutableList.of(
      "ID",
      "NAME",
      "SCORE"
    );

    List<String> typeName = ImmutableList.of(
      "INT",
      "STRING",
      "DOUBLE"
    );

    List<Integer> typeValue = ImmutableList.of(
      1,
      2,
      3
    );

    List<ColumnType> expectedColumns = new ArrayList<>();
    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    for (int i = 0; i < columns.size(); i++) {
      resultSetMetaData.setColumnName(i + 1, columns.get(i));
      resultSetMetaData.setColumnTypeName(i + 1, typeName.get(i));
      resultSetMetaData.setColumnType(i + 1, typeValue.get(i));
      expectedColumns.add(new ColumnType(columns.get(i), typeName.get(i), typeValue.get(i)));
    }

    MockResultSet resultSet = new MockResultSet("data");
    Set<String> columnNamesSet = new HashSet<String>();
    columnNamesSet.addAll(columns);
    resultSet.addColumns(columnNamesSet);
    resultSet.setResultSetMetaData(resultSetMetaData);

    List<ColumnType> actualColumns = AbstractDBSink.getMatchedColumnTypeList(resultSet, subsetCol);

    // Assert that all actual fields are present in the expected fields
    for (ColumnType acColType : actualColumns) {
      Assert.assertTrue(expectedColumns.contains(acColType));
    }
  }
}

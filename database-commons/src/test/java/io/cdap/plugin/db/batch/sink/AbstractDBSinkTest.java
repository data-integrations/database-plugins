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
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import io.cdap.plugin.db.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test class for abstract sink.
 */
public class AbstractDBSinkTest {

  @Test
  public void testGetMatchedColumnTypeList() throws SQLException {
    List<String> columns = ImmutableList.of(
      "ID",
      "NAME",
      "AGE"
    );

    List<ColumnType> expectedColumns = new ArrayList<>();

    MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnCount(columns.size());

    for (int i = 0; i < columns.size(); i++) {
      String name = columns.get(i);
      resultSetMetaData.setColumnName(i + 1, columns.get(i));
      resultSetMetaData.setColumnTypeName(i + 1, "STRING");
      resultSetMetaData.setColumnType(i + 1, i);
      expectedColumns.add(new ColumnType(name, "STRING", i));
    }

    List<ColumnType> result = AbstractDBSink.getMatchedColumnTypeList(resultSetMetaData, columns);

    Assert.assertEquals(expectedColumns, result);
  }

  @Test
  public void testGetMismatchColumnTypeList() throws SQLException {
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

    try {
      AbstractDBSink.getMatchedColumnTypeList(resultSetMetaData, wrongColumns);
      Assert.fail(String.format("Expected to throw %s", IllegalArgumentException.class.getName()));
    } catch (IllegalArgumentException e) {
      String errorMessage = "Missing column 'MY_ID' in SQL table";
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }
}

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

package co.cask.db2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class TestApp {

  public static void main(String[] args) throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:db2://localhost:50000/sample", "db2inst1", "cdap");

    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery("select * from my_table");


    resultSet.next();

    ResultSetMetaData metaData = resultSet.getMetaData();

    for (int i = 1; i <= metaData.getColumnCount(); i++) {

      System.out.println(String.format("column name : [%s], type: [%s], typeName: [%s]",
                                       metaData.getColumnName(i),
                                       metaData.getColumnType(i),
                                       metaData.getColumnTypeName(i)
      ));

    }
    resultSet.getMetaData();
  }
}

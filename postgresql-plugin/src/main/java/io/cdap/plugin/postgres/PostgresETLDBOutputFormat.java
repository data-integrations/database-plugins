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

package io.cdap.plugin.postgres;

import io.cdap.plugin.db.sink.ETLDBOutputFormat;

/**
 * Class that extends {@link ETLDBOutputFormat} to implement the abstract methods
 */
public class PostgresETLDBOutputFormat extends ETLDBOutputFormat {

  /**
   * This method is used to construct the upsert query for PostgreSQL
   * Example - INSERT INTO my_table (id, name, age)
   * VALUES (123, 'John', 25) ON CONFLICT (id)
   * DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age;
   * @param table - Name of the table
   * @param fieldNames - All the columns of the table
   * @param listKeys - The columns to be updated
   * @return Upsert query in the form of string
   */
  @Override
  public String constructUpsertQuery(String table, String[] fieldNames, String[] listKeys) {
    if (listKeys == null) {
      throw new IllegalArgumentException("Column names to be updated may not be null");
    } else if (fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    } else {
      StringBuilder query = new StringBuilder();
      query.append("INSERT INTO ").append(table);
      int i;
      if (fieldNames.length > 0 && fieldNames[0] != null) {
        query.append(" (");
        for (i = 0; i < fieldNames.length; ++i) {
          query.append(fieldNames[i]);
          if (i != fieldNames.length - 1) {
            query.append(",");
          }
        }

        query.append(") VALUES (");

        for (i = 0; i < fieldNames.length; ++i) {
          query.append("?");
          if (i != fieldNames.length - 1) {
            query.append(",");
          }
        }

        query.append(")").append(" ON CONFLICT ");
      }

      if (listKeys.length > 0 && listKeys[0] != null) {
        query.append("(");
        for (i = 0; i < listKeys.length; ++i) {
          query.append(listKeys[i]);
          if (i != listKeys.length - 1) {
            query.append(", ");
          }
        }

        query.append(")").append(" DO UPDATE SET ");

        for (i = 0; i < listKeys.length; ++i) {
          query.append(listKeys[i]).append(" = EXCLUDED.").append(listKeys[i]);
          if (i != listKeys.length - 1) {
            query.append(", ");
          }
        }
      }

      query.append(";");
      return query.toString();
    }
  }
}

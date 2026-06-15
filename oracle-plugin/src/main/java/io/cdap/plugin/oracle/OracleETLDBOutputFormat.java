/*
 * Copyright © 2026 Cask Data, Inc.
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

import io.cdap.plugin.db.sink.ETLDBOutputFormat;

/**
 * Class that extends {@link ETLDBOutputFormat} to implement the abstract methods
 */
public class OracleETLDBOutputFormat extends ETLDBOutputFormat {

  /**
   * This method is used to construct the upsert query for Oracle using MERGE statement.
   * Example - MERGE INTO my_table target
   * USING (SELECT ? AS id, ? AS name, ? AS age FROM dual) source
   * ON (target.id = source.id)
   * WHEN MATCHED THEN UPDATE SET target.name = source.name, target.age = source.age
   * WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (source.id, source.name, source.age)
   * @param table - Name of the table
   * @param fieldNames - All the columns of the table
   * @param listKeys - The columns used as keys for matching
   * @return Upsert query in the form of string
   */
  @Override
  public String constructUpsertQuery(String table, String[] fieldNames, String[] listKeys) {
    if (listKeys == null) {
      throw new IllegalArgumentException(
        "'Relation Table Key' must be specified for upsert operations. " +
        "Please provide the list of key columns used to match records in the target table.");
    } else if (fieldNames == null) {
      throw new IllegalArgumentException(
        "'Field Names' must be specified for upsert operations. " +
        "Please provide the list of columns to be written to the target table.");
    } else {
      StringBuilder query = new StringBuilder();

      // MERGE INTO target_table target
      query.append("MERGE INTO ").append(table).append(" target ");

      // USING (SELECT ? AS col1, ? AS col2, ... FROM dual) source
      query.append("USING (SELECT ");
      for (int i = 0; i < fieldNames.length; ++i) {
        query.append("? AS ").append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(", ");
        }
      }
      query.append(" FROM dual) source ");

      // ON (target.key1 = source.key1 AND target.key2 = source.key2 ...)
      query.append("ON (");
      for (int i = 0; i < listKeys.length; ++i) {
        query.append("target.").append(listKeys[i]).append(" = source.").append(listKeys[i]);
        if (i != listKeys.length - 1) {
          query.append(" AND ");
        }
      }
      query.append(") ");

      // WHEN MATCHED THEN UPDATE SET target.col1 = source.col1, target.col2 = source.col2 ...
      // Only update non-key columns
      query.append("WHEN MATCHED THEN UPDATE SET ");
      boolean firstUpdateColumn = true;
      for (String fieldName : fieldNames) {
        boolean isKeyColumn = false;
        for (String listKey : listKeys) {
          String listKeyNoQuote = listKey.replace("\"", "");
          if (listKeyNoQuote.equals(fieldName)) {
            isKeyColumn = true;
            break;
          }
        }
        if (!isKeyColumn) {
          if (!firstUpdateColumn) {
            query.append(", ");
          }
          query.append("target.").append(fieldName).append(" = source.").append(fieldName);
          firstUpdateColumn = false;
        }
      }

      // WHEN NOT MATCHED THEN INSERT (col1, col2, ...) VALUES (source.col1, source.col2, ...)
      query.append(" WHEN NOT MATCHED THEN INSERT (");
      for (int i = 0; i < fieldNames.length; ++i) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(", ");
        }
      }
      query.append(") VALUES (");
      for (int i = 0; i < fieldNames.length; ++i) {
        query.append("source.").append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(", ");
        }
      }
      query.append(")");

      return query.toString();
    }
  }

  @Override
  public String constructUpdateQuery(String table, String[] fieldNames, String[] listKeys) {
    // Oracle JDBC does not accept a trailing semicolon in prepared statements.
    String query = super.constructUpdateQuery(table, fieldNames, listKeys);
    if (query.endsWith(";")) {
      return query.substring(0, query.length() - 1);
    }
    return query;
  }
}

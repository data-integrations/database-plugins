/*
 * Copyright © 2025 Cask Data, Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Class that extends {@link ETLDBOutputFormat} to implement the abstract methods for Oracle.
 */
public class OracleETLDBOutputFormat extends ETLDBOutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(OracleETLDBOutputFormat.class);

  /**
   * This method is used to construct the MERGE query for Oracle.
   * Example - MERGE INTO target_table USING source_table ON (target_table.id = source_table.id)
   * WHEN MATCHED THEN UPDATE SET target_table.name = source_table.name, target_table.age = source_table.age
   * WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (source_table.id, source_table.name, source_table.age);
   *
   * In this context, the source is a single row represented by placeholders.
   *
   * @param table      Name of the target table.
   * @param fieldNames All the columns of the table.
   * @param mergeKeys  The key columns to use for the ON condition.
   * @return MERGE query in the form of a string.
   */
  @Override
  public String constructUpsertQuery(String table, String[] fieldNames, String[] mergeKeys) {
    LOG.debug("Constructing upsert query for table: {}, fields: {}, keys: {}",
        table, Arrays.toString(fieldNames), Arrays.toString(mergeKeys));
    if (mergeKeys == null || mergeKeys.length == 0) {
      throw new IllegalArgumentException("Merge keys must be specified for MERGE operation.");
    }
    if (fieldNames == null || fieldNames.length == 0) {
      throw new IllegalArgumentException("Field names must be specified for MERGE operation.");
    }

    String targetTable = table;
    String sourceTable = "DUAL"; // We are merging a single row

    StringBuilder query = new StringBuilder();
    query.append("MERGE INTO ").append(targetTable).append(" TGT");
    query.append(" USING (SELECT ");
    for (int i = 0; i < fieldNames.length; i++) {
      query.append("? ").append(fieldNames[i]);
      if (i < fieldNames.length - 1) {
        query.append(", ");
      }
    }
    query.append(" FROM DUAL) SRC");

    query.append(" ON (");
    for (int i = 0; i < mergeKeys.length; i++) {
      query.append("TGT.").append(mergeKeys[i]).append(" = SRC.").append(mergeKeys[i]);
      if (i < mergeKeys.length - 1) {
        query.append(" AND ");
      }
    }
    query.append(")");

    // UPDATE clause
    query.append(" WHEN MATCHED THEN UPDATE SET ");
    boolean firstUpdate = true;
    for (String fieldName : fieldNames) {
      if (!Arrays.asList(mergeKeys).contains(fieldName)) {
        if (!firstUpdate) {
          query.append(", ");
        }
        query.append("TGT.").append(fieldName).append(" = SRC.").append(fieldName);
        firstUpdate = false;
      }
    }
    if (firstUpdate) {
      // Should not happen if there are non-key fields, but good to handle.
      // If all fields are keys, we can put a dummy update.
      query.append("TGT.").append(mergeKeys[0]).append(" = SRC.").append(mergeKeys[0]);
    }

    // INSERT clause
    query.append(" WHEN NOT MATCHED THEN INSERT (");
    query.append(Arrays.stream(fieldNames).collect(Collectors.joining(", ")));
    query.append(") VALUES (");
    query.append(Arrays.stream(fieldNames).map(f -> "SRC." + f).collect(Collectors.joining(", ")));
    query.append(")");

    String resultQuery = query.toString();
    LOG.debug("Constructed upsert query: {}", resultQuery);
    return resultQuery;
  }
}

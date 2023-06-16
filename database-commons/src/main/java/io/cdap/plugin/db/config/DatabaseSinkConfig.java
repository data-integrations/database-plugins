/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.db.config;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.db.Operation;

import java.util.List;

/**
 * Interface for DB Sink plugin config
 */
public interface DatabaseSinkConfig extends DatabaseConnectionConfig {

  /**
   * @return the reference name of the sink stage
   */
  String getReferenceName();

  /**
   * @return the transaction isolation level
   */
  String getTransactionIsolationLevel();

  /**
   * @return the initial query to be run upon connecting to database
   */
  List<String> getInitQueries();

  /**
   * @return true if none of the connection parameters is macro, otherwise false
   */
  boolean canConnect();

  /**
   * @return the table name
   */
  String getTableName();

  /**
   * @return the schema name
   */
  String getDBSchemaName();

  /**
   * Adds escape characters (back quotes, double quotes, etc.) to the table name for
   * databases with case-sensitive identifiers.
   *
   * @return tableName with leading and trailing escape characters appended.
   * Default implementation returns unchanged table name string.
   */
  String getEscapedTableName();

  /**
   * Adds escape characters (back quotes, double quotes, etc.) to the database schema name for
   * databases with case-sensitive identifiers.
   *
   * @return dBSchemaName with leading and trailing escape characters appended.
   * Default implementation returns unchanged table name string.
   */
  String getEscapedDbSchemaName();

  /**
   * Validate the sink config
   *
   * @param collector the failure collector
   */
  default void validate(FailureCollector collector) {
    // no-op
  }

  /**
   * @return the operation to be performed on the query
   */
  Operation getOperationName();

  /**
   * @return the column names on which update and upsert are to be performed.
   */
  String getRelationTableKey();
}

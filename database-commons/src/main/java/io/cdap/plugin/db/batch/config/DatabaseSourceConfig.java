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

package io.cdap.plugin.db.batch.config;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.List;

/**
 * Interface for DB Source plugin config
 */
public interface DatabaseSourceConfig extends DatabaseConnectionConfig {

  /**
   * @return the reference name of the source stage
   */
  String getReferenceName();

  /**
   * validate the configuration
   *
   * @param collector the failure collector
   */
  void validate(FailureCollector collector);

  /**
   * @return the source schema
   */
  Schema getSchema();

  /**
   * @return the import query
   */
  String getImportQuery();

  /**
   * @return the bounding query
   */
  String getBoundingQuery();

  /**
   * @return the transaction isolation level
   */
  String getTransactionIsolationLevel();

  /**
   * @return the number of splits
   */
  Integer getNumSplits();

  /**
   * @return the split column name
   */
  String getSplitBy();

  /**
   * validate whether configured schema is compatible with the actual schema got from database
   *
   * @param schemaFromDB the actual schema got from database
   * @param collector    the failure collector
   */
  void validateSchema(Schema schemaFromDB, FailureCollector collector);

  /**
   * @return the initial query to be run upon connecting to database
   */
  List<String> getInitQueries();
}

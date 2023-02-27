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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Main Interface to validate db fields.
 */
public interface FieldsValidator {

  /**
   * Checks if fields from schema are compatible to be written into database.
   *
   * @param inputSchema input schema.
   * @param resultSet   resultSet with database fields.
   */
  void validateFields(Schema inputSchema, ResultSet resultSet, FailureCollector collector) throws SQLException;

  /**
   * Checks if field is compatible to be written into database column of the given sql index.
   *
   * @param field    field of the explicit input schema.
   * @param metadata resultSet metadata.
   * @param index    sql column index.
   * @return 'true' if field is compatible to be written, 'false' otherwise.
   */
  boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException;

  /**
   * Checks if field is compatible to be written into database column of the given sql index.
   *
   * @param fieldType        field type.
   * @param fieldLogicalType filed logical type.
   * @param sqlType          code of sql type.
   * @return 'true' if field is compatible to be written, 'false' otherwise.
   */
  boolean isFieldCompatible(Schema.Type fieldType, Schema.LogicalType fieldLogicalType, int sqlType);
}

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

package co.cask;

import co.cask.cdap.api.data.schema.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Main Interface to read db specific types.
 */
public interface SchemaReader {

  /**
   *  Given the result set, get the metadata of the result set and return
   *  list of {@link co.cask.cdap.api.data.schema.Schema.Field},
   *  where name of the field is same as column name and type of the field is obtained using
   *  {@link SchemaReader#getSchema(ResultSetMetaData, int)}
   * @param resultSet Sql query result set
   * @param schemaStr schema string to override resultant schema
   * @return list of schema fields
   * @throws SQLException
   */
  List<Schema.Field> getSchemaFields(ResultSet resultSet, @Nullable String schemaStr) throws SQLException;

  /**
   * Given the result set, get the metadata of the result set and return
   * list of {@link co.cask.cdap.api.data.schema.Schema.Field},
   * where name of the field is same as column name and type of the field is obtained using
   * {@link SchemaReader#getSchema(ResultSetMetaData, int)}
   *
   * @param resultSet result set of executed query
   * @return list of schema fields
   * @throws SQLException
   */
  List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException;

  /**
   * Given a sql metadata return schema type
   * @param metadata resultSet metadata
   * @param index column index
   * @return CDAP schema
   * @throws SQLException
   */
  Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException;
}

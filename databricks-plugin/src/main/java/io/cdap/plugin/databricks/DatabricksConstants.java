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

package io.cdap.plugin.databricks;

/** Databricks constants. */
public final class DatabricksConstants {

  private DatabricksConstants() {
  }

  public static final String PLUGIN_NAME = "Databricks";
  public static final String DRIVER_CLASS_NAME = "com.databricks.client.jdbc.Driver";
  public static final String DATABRICKS_CONNECTION_STRING_FORMAT =
    "jdbc:databricks://%s:%d;HttpPath=%s;";
  public static final String DATABRICKS_DB_CONNECTION_STRING_FORMAT =
    "jdbc:databricks://%s:%d/%s;HttpPath=%s;";
}

/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.mariadb;

/**
 * MariaDB constants.
 */
public final class MariadbConstants {
  private MariadbConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static final String PLUGIN_NAME = "Mariadb";
  public static final String AUTO_RECONNECT = "autoReconnect";
  public static final String USE_COMPRESSION = "useCompression";
  public static final String SQL_MODE = "sqlMode";
  public static final String USE_SSL = "useSSL";
  public static final String USE_ANSI_QUOTES = "useAnsiQuotes";
  public static final String NO_SSL_OPTION = "No";
  public static final String REQUIRE_SSL_OPTION = "Yes";
  public static final String KEY_STORE = "keyStore";
  public static final String KEY_STORE_PASSWORD = "keyStorePassword";
  public static final String TRUST_STORE = "trustStore";
  public static final String TRUST_STORE_PASSWORD = "trustStorePassword";
  public static final String MARIADB_CONNECTION_STRING_FORMAT = "jdbc:mariadb://%s:%s/%s";

  /**
   * Query to set SQL_MODE system variable.
   */
  public static final String SET_SQL_MODE_QUERY_FORMAT = "SET SESSION sql_mode = '%s';";

  /**
   * Query to append 'ANSI_QUOTES' sql mode to the current value of SQL_MODE system variable.
   */
  public static final String ANSI_QUOTES_QUERY = "SET SESSION sql_mode = (CONCAT(@@sql_mode , ',', 'ANSI_QUOTES'));";
}

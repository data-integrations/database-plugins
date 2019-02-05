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

package co.cask.mysql;

/**
 * MySQL Constants.
 */
public final class MysqlConstants {
  private MysqlConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static final String PLUGIN_NAME = "Mysql";
  public static final String AUTO_RECONNECT = "autoReconnect";
  public static final String ALLOW_MULTIPLE_QUERIES = "allowMultiQueries";
  public static final String MYSQL_CONNECTION_STRING_FORMAT = "jdbc:mysql://%s:%s/%s";
}

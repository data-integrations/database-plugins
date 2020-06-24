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

package io.cdap.plugin.cloudsql.mysql;

/** CloudSQL MySQL constants. */
public final class CloudSQLMySQLConstants {

  private CloudSQLMySQLConstants() {
  }

  public static final String PLUGIN_NAME = "CloudSQLMySQL";
  public static final String CONNECTION_NAME = "connectionName";
  public static final String CONNECTION_NAME_PATTERN = "[a-z0-9-]+:[a-z0-9-]+:[a-z0-9-]+";
  public static final String CONNECTION_TIMEOUT = "connectionTimeout";
  public static final String PUBLIC_INSTANCE = "public";
  public static final String PUBLIC_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT =
      "jdbc:mysql:///%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.mysql.SocketFactory";
  public static final String INSTANCE_TYPE = "instanceType";
  public static final String PRIVATE_INSTANCE = "private";
  public static final String PRIVATE_CLOUDSQL_MYSQL_CONNECTION_STRING_FORMAT = "jdbc:mysql://%s/%s";
}

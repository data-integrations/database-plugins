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

package io.cdap.plugin.jdbc;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.LinkedHashMap;

/**
 * URL Parser for DB
 */

public class DatabaseURLParser {
  // TODO: Implement Parsing Logic
//  private static final String MYSQL_JDBC_URL_PREFIX = "jdbc:mysql";
//  private static final String ORACLE_JDBC_URL_PREFIX = "jdbc:oracle";
//  private static final String H2_JDBC_URL_PREFIX = "jdbc:h2";
//  private static final String POSTGRESQL_JDBC_URL_PREFIX = "jdbc:postgresql";
//  private static final String MARIADB_JDBC_URL_PREFIX = "jdbc:mariadb";
//  private static final String SQLSERVER_JDBC_URL_PREFIX = "jdbc:sqlserver";
//  private static final String DB2_JDBC_URL_PREFIX = "jdbc:db2";
//  private static final String AS400_JDBC_URL_PREFIX = "jdbc:as400";
  private static String dbScheme;
  private static String dbHost;
  private static int dbPort;
  private static String dbPath;
  private static String dbName;
  private static String tableName;

  public static URI parseURL(@NotNull String url) {
    String cleanURI = url.substring(5);

    URI uri = URI.create(cleanURI);
    dbScheme = uri.getScheme();
    dbHost = uri.getHost();
    dbPort = uri.getPort();
    dbPath = uri.getPath();
    return uri;
  }
}

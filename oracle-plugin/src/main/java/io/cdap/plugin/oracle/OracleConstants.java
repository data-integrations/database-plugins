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

package io.cdap.plugin.oracle;

import javax.annotation.Nullable;

/**
 * Oracle Constants.
 */
public final class OracleConstants {
  private OracleConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static final String PLUGIN_NAME = "Oracle";
  public static final String ORACLE_CONNECTION_STRING_SID_FORMAT = "jdbc:oracle:thin:@%s:%s:%s";
  public static final String ORACLE_CONNECTION_STRING_SERVICE_NAME_FORMAT = "jdbc:oracle:thin:@//%s:%s/%s";
  // Connection formats to accept protocol (e.g., jdbc:oracle:thin:@<protocol>://<host>:<port>/<SID>)
  public static final String ORACLE_CONNECTION_STRING_SID_FORMAT_WITH_PROTOCOL = "jdbc:oracle:thin:@%s:%s:%s/%s";
  public static final String ORACLE_CONNECTION_STRING_SERVICE_NAME_FORMAT_WITH_PROTOCOL =
      "jdbc:oracle:thin:@%s://%s:%s/%s";
  public static final String ORACLE_CONNECTION_STRING_TNS_FORMAT = "jdbc:oracle:thin:@%s";
  public static final String DEFAULT_BATCH_VALUE = "defaultBatchValue";
  public static final String DEFAULT_ROW_PREFETCH = "defaultRowPrefetch";
  public static final String SERVICE_CONNECTION_TYPE = "service";
  public static final String CONNECTION_TYPE = "connectionType";
  public static final String ROLE = "role";
  public static final String NAME_DATABASE = "database";
  public static final String TNS_CONNECTION_TYPE = "tns";
  public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";
  public static final String USE_SSL = "useSSL";
  public static final String TREAT_AS_OLD_TIMESTAMP = "treatAsOldTimestamp";
  public static final String TREAT_PRECISIONLESSNUM_AS_DECI = "treatPrecisionlessNumAsDeci";
  public static final String TREAT_TIMESTAMP_LTZ_AS_TIMESTAMP = "treatTimestampLTZAsTimestamp";

  /**
   * Constructs the Oracle connection string based on the provided connection type, host, port, and database.
   * If SSL is enabled, the connection protocol will be "tcps" instead of "tcp".
   *
   * @param connectionType TNS/Service/SID
   * @param host Host name of the oracle server
   * @param port Port of the oracle server
   * @param database Database to connect to
   * @param useSSL Whether SSL/TLS is required(YES/NO)
   * @return Connection String based on the given parameters and connection type.
   */
  public static String getConnectionString(String connectionType,
                                           @Nullable String host,
                                           @Nullable int port,
                                           String database,
                                           @Nullable Boolean useSSL) {
    // Use protocol as "tcps" when SSL is requested or else use "tcp".
    String connectionProtocol;
    boolean isSSLEnabled = false;
    if (useSSL != null && useSSL) {
      connectionProtocol = "tcps";
      isSSLEnabled = true;
    } else {
      connectionProtocol = "tcp";
    }

    switch (connectionType.toLowerCase()) {
      case OracleConstants.TNS_CONNECTION_TYPE:
        // TNS connection doesn't require protocol
        return String.format(OracleConstants.ORACLE_CONNECTION_STRING_TNS_FORMAT, database);
      case OracleConstants.SERVICE_CONNECTION_TYPE:
        // Create connection string for SERVICE type.
        return getConnectionStringWithService(host, port, database, connectionProtocol, isSSLEnabled);
      default:
        // Default to SID format if no matching case is found.
        return getConnectionStringWithSID(host, port, database, connectionProtocol, isSSLEnabled);
    }
  }

  /**
   * Constructs the connection string for a SERVICE connection type.
   *
   * @param host Host name of the Oracle server.
   * @param port Port of the Oracle server.
   * @param database Database name to connect to.
   * @param connectionProtocol Protocol to use for the connection ("tcp" or "tcps").
   * @param isSSLEnabled Indicates if SSL is enabled.
   * @return Formatted connection string for a SERVICE connection.
   */
  private static String getConnectionStringWithService(@Nullable String host,
                                                       @Nullable int port,
                                                       String database,
                                                       String connectionProtocol,
                                                       boolean isSSLEnabled) {
    // Choose the appropriate format based on whether SSL is enabled.
    if (isSSLEnabled) {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SERVICE_NAME_FORMAT_WITH_PROTOCOL,
          connectionProtocol, host, port, database);
    }
    return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SERVICE_NAME_FORMAT,
        host, port, database);
  }

  /**
   * Constructs the connection string for a SID connection type.
   *
   * @param host Host name of the Oracle server.
   * @param port Port of the Oracle server.
   * @param database Database name to connect to.
   * @param connectionProtocol Protocol to use for the connection ("tcp" or "tcps").
   * @param isSSLEnabled Indicates if SSL is enabled.
   * @return Formatted connection string for a SID connection.
   */
  private static String getConnectionStringWithSID(@Nullable String host,
                                                   @Nullable int port,
                                                   String database,
                                                   String connectionProtocol,
                                                   boolean isSSLEnabled) {
    // Choose the appropriate format based on whether SSL is enabled.
    if (isSSLEnabled) {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SID_FORMAT_WITH_PROTOCOL,
          connectionProtocol, host, port, database);
    }
    return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SID_FORMAT,
        host, port, database);
  }
}

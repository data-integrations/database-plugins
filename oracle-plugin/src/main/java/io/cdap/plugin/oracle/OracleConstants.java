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
  public static final String ORACLE_CONNECTION_STRING_TNS_FORMAT = "jdbc:oracle:thin:@%s";
  public static final String DEFAULT_BATCH_VALUE = "defaultBatchValue";
  public static final String DEFAULT_ROW_PREFETCH = "defaultRowPrefetch";
  public static final String SERVICE_CONNECTION_TYPE = "service";
  public static final String CONNECTION_TYPE = "connectionType";
  public static final String ROLE = "role";
  public static final String NAME_DATABASE = "database";
  public static final String TNS_CONNECTION_TYPE = "TNS";
  public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";

  /**
   * Returns the Connection String for the given ConnectionType.
   *
   * @param connectionType TNS/Service/SID
   * @param host Host name of the oracle server
   * @param port Port of the oracle server
   * @param database Database to connect to
   * @return Connection String based on the given ConnectionType
   */
  public static String getConnectionString(String connectionType,
                                           @Nullable String host,
                                           @Nullable int port,
                                           String database) {
    if (OracleConstants.TNS_CONNECTION_TYPE.equalsIgnoreCase(connectionType)) {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_TNS_FORMAT, database);
    }
    if (OracleConstants.SERVICE_CONNECTION_TYPE.equalsIgnoreCase(connectionType)) {
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SERVICE_NAME_FORMAT,
          host, port, database);
    }
    return String.format(OracleConstants.ORACLE_CONNECTION_STRING_SID_FORMAT,
        host, port, database);
  }
}

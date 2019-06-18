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

package io.cdap.plugin.mssql;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * SqlServer util methods.
 */
public final class SqlServerUtil {

  private SqlServerUtil() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Composes immutable map of the SQL Server specific arguments.
   *
   * @param instanceName           the SQL Server instance name to connect to.
   * @param authenticationType     used to specify which SQL authentication method to use for connection.
   * @param applicationIntent      used to specify the application workload type when connecting to a server.
   * @param connectTimeout         number of seconds the driver should wait before timing out a failed  connection.
   * @param columnEncryption       used to specify if Always Encrypted (AE) feature must be enabled.
   * @param encrypt                used to specify if encryption must be enabled.
   * @param trustServerCertificate used to specify if server certificate must be trusted without validation.
   * @param workstationId          identifies the specific workstation in SQL Server profiling and logging tools.
   * @param failoverPartner        name of the failover server used in a database mirroring configuration.
   * @param packetSize             network packet size used to communicate with SQL Server, specified in bytes.
   * @param queryTimeout           number of seconds to wait before a timeout has occurred on a query.
   * @return immutable map of the SQL Server specific arguments
   */
  public static Map<String, String> composeDbSpecificArgumentsMap(String instanceName,
                                                                  String authenticationType,
                                                                  String applicationIntent,
                                                                  Integer connectTimeout,
                                                                  Boolean columnEncryption,
                                                                  Boolean encrypt,
                                                                  Boolean trustServerCertificate,
                                                                  String workstationId,
                                                                  String failoverPartner,
                                                                  Integer packetSize,
                                                                  Integer queryTimeout) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    if (!Strings.isNullOrEmpty(instanceName)) {
      builder.put(SqlServerConstants.INSTANCE_NAME, String.valueOf(instanceName));
    }
    if (SqlServerConstants.AD_PASSWORD_OPTION.equals(authenticationType)) {
      builder.put(SqlServerConstants.AUTHENTICATION, SqlServerConstants.AD_PASSWORD_OPTION);
    }
    if (!Strings.isNullOrEmpty(applicationIntent)) {
      builder.put(SqlServerConstants.APPLICATION_INTENT, applicationIntent);
    }
    if (connectTimeout != null) {
      builder.put(SqlServerConstants.CONNECT_TIMEOUT, String.valueOf(connectTimeout));
    }
    if (columnEncryption != null && columnEncryption) {
      builder.put(SqlServerConstants.COLUMN_ENCRYPTION, SqlServerConstants.COLUMN_ENCRYPTION_ENABLED);
    } else if (columnEncryption != null) {
      builder.put(SqlServerConstants.COLUMN_ENCRYPTION, SqlServerConstants.COLUMN_ENCRYPTION_DISABLED);
    }
    if (encrypt != null) {
      builder.put(SqlServerConstants.ENCRYPT, String.valueOf(encrypt));
    }
    if (trustServerCertificate != null) {
      builder.put(SqlServerConstants.TRUST_SERVER_CERTIFICATE, String.valueOf(trustServerCertificate));
    }
    if (!Strings.isNullOrEmpty(workstationId)) {
      builder.put(SqlServerConstants.WORKSTATION_ID, workstationId);
    }
    if (!Strings.isNullOrEmpty(failoverPartner)) {
      builder.put(SqlServerConstants.FAILOVER_PARTNER, failoverPartner);
    }
    if (packetSize != null) {
      builder.put(SqlServerConstants.PACKET_SIZE, String.valueOf(packetSize));
    }
    if (queryTimeout != null) {
      builder.put(SqlServerConstants.QUERY_TIMEOUT, String.valueOf(queryTimeout));
    }

    return builder.build();
  }

  /**
   * Creates SQL Server specific JDBC connection string.
   *
   * @param host     server host.
   * @param port     server port.
   * @param database database name.
   * @return SQL Server specific JDBC connection string
   */
  public static String getConnectionString(String host, Integer port, String database) {
    return String.format(SqlServerConstants.SQL_SERVER_CONNECTION_STRING_FORMAT, host, port, database);
  }
}

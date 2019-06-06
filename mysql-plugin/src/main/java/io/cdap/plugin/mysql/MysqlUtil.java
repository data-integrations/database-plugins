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

package io.cdap.plugin.mysql;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * MySQL util methods.
 */
public final class MysqlUtil {
  private MysqlUtil() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Composes immutable map of the MySQL specific arguments.
   *
   * @param autoReconnect                     should the driver try to re-establish stale and/or dead connections.
   * @param useCompression                    specifies if compression must be enabled.
   * @param useSSL                            specifies if SSL encryption must be turned on.
   * @param clientCertificateKeyStoreUrl      URL of the client certificate KeyStore.
   * @param clientCertificateKeyStorePassword password for the client certificates KeyStore.
   * @param trustCertificateKeyStoreUrl       URL of the trusted root certificate KeyStore.
   * @param trustCertificateKeyStorePassword  password for the trusted root certificates KeyStore.
   * @return immutable map of the MySQL specific arguments
   */
  public static Map<String, String> composeDbSpecificArgumentsMap(Boolean autoReconnect,
                                                                  Boolean useCompression,
                                                                  String useSSL,
                                                                  String clientCertificateKeyStoreUrl,
                                                                  String clientCertificateKeyStorePassword,
                                                                  String trustCertificateKeyStoreUrl,
                                                                  String trustCertificateKeyStorePassword) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    if (autoReconnect != null) {
      builder.put(MysqlConstants.AUTO_RECONNECT, String.valueOf(autoReconnect));
    }
    if (useCompression != null) {
      builder.put(MysqlConstants.USE_COMPRESSION, String.valueOf(useCompression));
    }
    if (MysqlConstants.REQUIRE_SSL_OPTION.equals(useSSL)) {
      builder.put(MysqlConstants.USE_SSL, "true");
    } else if (MysqlConstants.NO_SSL_OPTION.equals(useSSL)) {
      builder.put(MysqlConstants.USE_SSL, "false");
    }
    if (clientCertificateKeyStoreUrl != null) {
      builder.put(MysqlConstants.CLIENT_CERT_KEYSTORE_URL, String.valueOf(clientCertificateKeyStoreUrl));
    }
    if (clientCertificateKeyStorePassword != null) {
      builder.put(MysqlConstants.CLIENT_CERT_KEYSTORE_PASSWORD, String.valueOf(clientCertificateKeyStorePassword));
    }
    if (trustCertificateKeyStoreUrl != null) {
      builder.put(MysqlConstants.TRUST_CERT_KEYSTORE_URL, String.valueOf(trustCertificateKeyStoreUrl));
    }
    if (trustCertificateKeyStorePassword != null) {
      builder.put(MysqlConstants.TRUST_CERT_KEYSTORE_PASSWORD, String.valueOf(trustCertificateKeyStorePassword));
    }

    return builder.build();
  }

  /**
   * Creates MySQL specific JDBC connection string.
   *
   * @param host     server host.
   * @param port     server port.
   * @param database database name.
   * @return MySQL specific JDBC connection string
   */
  public static String getConnectionString(String host, Integer port, String database) {
    return String.format(MysqlConstants.MYSQL_CONNECTION_STRING_FORMAT, host, port, database);
  }
}

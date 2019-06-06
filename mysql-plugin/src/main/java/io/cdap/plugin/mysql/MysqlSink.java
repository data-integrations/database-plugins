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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.batch.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink support for a MySQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MysqlConstants.PLUGIN_NAME)
@Description("Writes records to a MySQL table. Each record will be written in a row in the table")
public class MysqlSink extends AbstractDBSink {

  private final MysqlSinkConfig mysqlSinkConfig;

  public MysqlSink(MysqlSinkConfig mysqlSinkConfig) {
    super(mysqlSinkConfig);
    this.mysqlSinkConfig = mysqlSinkConfig;
  }

  /**
   * MySQL action configuration.
   */
  public static class MysqlSinkConfig extends DBSpecificSinkConfig {

    @Name(MysqlConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MysqlConstants.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Name(MysqlConstants.SQL_MODE)
    @Description("Override the default SQL_MODE session variable used by the server")
    @Nullable
    public String sqlMode;

    @Name(MysqlConstants.USE_SSL)
    @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
    @Nullable
    public String useSSL;

    @Name(MysqlConstants.CLIENT_CERT_KEYSTORE_URL)
    @Description("URL to the client certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String clientCertificateKeyStoreUrl;

    @Name(MysqlConstants.CLIENT_CERT_KEYSTORE_PASSWORD)
    @Description("Password for the client certificates KeyStore")
    @Nullable
    public String clientCertificateKeyStorePassword;

    @Name(MysqlConstants.TRUST_CERT_KEYSTORE_URL)
    @Description("URL to the trusted root certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String trustCertificateKeyStoreUrl;

    @Name(MysqlConstants.TRUST_CERT_KEYSTORE_PASSWORD)
    @Description("Password for the trusted root certificates KeyStore")
    @Nullable
    public String trustCertificateKeyStorePassword;

    @Override
    public String getConnectionString() {
      return MysqlUtil.getConnectionString(host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return MysqlUtil.composeDbSpecificArgumentsMap(autoReconnect, useCompression, useSSL,
                                                     clientCertificateKeyStoreUrl,
                                                     clientCertificateKeyStorePassword,
                                                     trustCertificateKeyStoreUrl,
                                                     trustCertificateKeyStorePassword);
    }

    @Override
    public List<String> getInitQueries() {
      if (!Strings.isNullOrEmpty(sqlMode)) {
        return Collections.singletonList(String.format(MysqlConstants.SET_SQL_MODE_QUERY_FORMAT, sqlMode));
      }
      return Collections.emptyList();
    }
  }
}

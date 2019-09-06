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

package io.cdap.plugin.mariadb;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.batch.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * MariaDB Sink
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MariadbConstants.PLUGIN_NAME)
@Description("Writes records to a MariaDB table. Each record will be written in a row in the table")
public class MariadbSink extends AbstractDBSink {

  private final MariadbSinkConfig mariadbSinkConfig;

  public MariadbSink(MariadbSinkConfig mariadbSinkConfig) {
    super(mariadbSinkConfig);
    this.mariadbSinkConfig = mariadbSinkConfig;
  }

  /**
   * MariaDB Sink Config
   */
  public static class MariadbSinkConfig extends DBSpecificSinkConfig {

    @Name(MariadbConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MariadbConstants.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Name(MariadbConstants.SQL_MODE)
    @Description("Override the default SQL_MODE session variable used by the server")
    @Nullable
    public String sqlMode;

    @Name(MariadbConstants.USE_SSL)
    @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
    @Nullable
    public String useSSL;

    @Name(MariadbConstants.KEY_STORE)
    @Description("URL to the client certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String keyStore;

    @Name(MariadbConstants.KEY_STORE_PASSWORD)
    @Description("Password for the client certificates KeyStore")
    @Nullable
    public String keyStorePassword;

    @Name(MariadbConstants.TRUST_STORE)
    @Description("URL to the trusted root certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String trustStore;

    @Name(MariadbConstants.TRUST_STORE_PASSWORD)
    @Description("Password for the trusted root certificates KeyStore")
    @Nullable
    public String trustStorePassword;

    @Override
    public String getConnectionString() {
      return MariadbUtil.getConnectionString(host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return MariadbUtil.composeDbSpecificArgumentsMap(autoReconnect, useCompression, useSSL,
              keyStore,
              keyStorePassword,
              trustStore,
              trustStorePassword);
    }

    @Override
    public List<String> getInitQueries() {
      return MariadbUtil.composeDbInitQueries(null, sqlMode);
    }
  }
}

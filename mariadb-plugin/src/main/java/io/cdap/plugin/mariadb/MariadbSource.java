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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.db.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.source.AbstractDBSource;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * MariaDB source.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MariadbConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class MariadbSource extends AbstractDBSource<MariadbSource.MariadbSourceConfig> {

  private final MariadbSourceConfig mariadbSourceConfig;

  /**
   * This is the constructor for MariadbSource.
   * @param mariadbSourceConfig It takes Mariadb source config object as the parameter.
   */
  public MariadbSource(MariadbSourceConfig mariadbSourceConfig) {
    super(mariadbSourceConfig);
    this.mariadbSourceConfig = mariadbSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return String.format(MariadbConstants.MARIADB_CONNECTION_STRING_FORMAT,
                         mariadbSourceConfig.host, mariadbSourceConfig.port, mariadbSourceConfig.database);
  }

  /**
   * MaraiDB source mariadbSourceConfig.
   */
  public static class MariadbSourceConfig extends DBSpecificSourceConfig {

    @Name(MariadbConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MariadbConstants.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Name(MariadbConstants.USE_SSL)
    @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
    @Nullable
    public String useSSL;

    @Name(MariadbConstants.USE_ANSI_QUOTES)
    @Description("Treats \" as an identifier quote character and not as a string quote character")
    @Nullable
    public Boolean useAnsiQuotes;

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
      return MariadbUtil.composeDbInitQueries(useAnsiQuotes);
    }
  }
}

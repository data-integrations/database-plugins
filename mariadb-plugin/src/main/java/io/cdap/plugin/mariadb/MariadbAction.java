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
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.batch.action.AbstractDBAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryConfig;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * MariaDB action that runs MariaDB command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(MariadbConstants.PLUGIN_NAME)
@Description("Action that runs a MariaDB command")
public class MariadbAction extends AbstractDBAction {

  private final MariadbActionConfig mariadbActionConfig;

  /**
   * This is the constructor for MariadbAction.
   * @param mariadbActionConfig It takes Mariadb Action config object as the parameter.
   */
  public MariadbAction(MariadbActionConfig mariadbActionConfig) {
    super(mariadbActionConfig, false);
    this.mariadbActionConfig = mariadbActionConfig;
  }

  /**
   * MariaDB Action Config.
   * TO DO
   * Most of these configs are common to all mariadb plugins. These needs to be put into common class to avoid
   * code duplication. https://issues.cask.co/browse/PLUGIN-358
   */
  public static class MariadbActionConfig extends DBSpecificQueryConfig {

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

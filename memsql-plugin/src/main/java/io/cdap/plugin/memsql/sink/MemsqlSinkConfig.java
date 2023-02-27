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

package io.cdap.plugin.memsql.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.config.DBSpecificSinkConfig;
import io.cdap.plugin.memsql.MemsqlConstants;
import io.cdap.plugin.memsql.MemsqlUtil;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config class for {@link MemsqlSink}.
 */
public class MemsqlSinkConfig extends DBSpecificSinkConfig {

  @Name(MemsqlConstants.AUTO_RECONNECT)
  @Description("Should the driver try to re-establish stale and/or dead connections")
  @Nullable
  public Boolean autoReconnect;

  @Name(MemsqlConstants.USE_COMPRESSION)
  @Description("Select this option for WAN connections")
  @Nullable
  public Boolean useCompression;

  @Name(MemsqlConstants.USE_SSL)
  @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
  @Nullable
  public String useSSL;

  @Name(MemsqlConstants.KEY_STORE)
  @Description("URL to the client certificate KeyStore (if not specified, use defaults)")
  @Nullable
  public String keyStore;

  @Name(MemsqlConstants.KEY_STORE_PASSWORD)
  @Description("Password for the client certificates KeyStore")
  @Nullable
  public String keyStorePassword;

  @Name(MemsqlConstants.TRUST_STORE)
  @Description("URL to the trusted root certificate KeyStore (if not specified, use defaults)")
  @Nullable
  public String trustStore;

  @Name(MemsqlConstants.TRUST_STORE_PASSWORD)
  @Description("Password for the trusted root certificates KeyStore")
  @Nullable
  public String trustStorePassword;

  @Override
  public String getConnectionString() {
    return MemsqlUtil.getConnectionString(host, port, database);
  }

  @Override
  public Map<String, String> getDBSpecificArguments() {
    return MemsqlUtil.composeDbSpecificArgumentsMap(autoReconnect, useCompression, useSSL,
                                                    keyStore,
                                                    keyStorePassword,
                                                    trustStore,
                                                    trustStorePassword);
  }
}

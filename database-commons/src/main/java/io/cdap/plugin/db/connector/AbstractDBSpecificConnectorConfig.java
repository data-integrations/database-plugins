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

package io.cdap.plugin.db.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.db.ConnectionConfig;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An abstract DB Specific Connector config that those DB specific plugin config can inherit
 */
public abstract class AbstractDBSpecificConnectorConfig extends AbstractDBConnectorConfig {

  @Name(ConnectionConfig.HOST)
  @Description("Database host")
  @Macro
  @Nullable
  protected String host;

  @Name(ConnectionConfig.PORT)
  @Description("Database port number")
  @Macro
  @Nullable
  protected Integer port;

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port == null ? getDefaultPort() : port;
  }

  protected abstract int getDefaultPort();

  public boolean canConnect() {
    return super.canConnect() && !containsMacro(ConnectionConfig.HOST) && !containsMacro(ConnectionConfig.PORT);
  }
}

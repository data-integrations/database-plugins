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
import io.cdap.plugin.db.TransactionIsolationLevel;

import java.util.HashMap;
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

  @Name(ConnectionConfig.TRANSACTION_ISOLATION_LEVEL)
  @Description("The transaction isolation level for the database session.")
  @Macro
  @Nullable
  protected String transactionIsolationLevel;

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

  @Override
  public Map<String, String> getAdditionalArguments() {
    Map<String, String> additonalArguments = new HashMap<>();
    if (getTransactionIsolationLevel() != null) {
      additonalArguments.put(TransactionIsolationLevel.CONF_KEY, getTransactionIsolationLevel());
    }
    return additonalArguments;
  }

  public String getTransactionIsolationLevel() {
    if (transactionIsolationLevel == null) {
      return null;
    }
    return TransactionIsolationLevel.Level.valueOf(transactionIsolationLevel).name();
  }
}


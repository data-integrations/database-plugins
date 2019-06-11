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
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.batch.action.AbstractDBAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Action that runs MSSQL command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(SqlServerConstants.PLUGIN_NAME)
@Description("Action that runs a MSSQL command")
public class SqlServerAction extends AbstractDBAction {

  private final SqlServerActionConfig sqlServerActionConfig;

  public SqlServerAction(SqlServerActionConfig sqlServerActionConfig) {
    super(sqlServerActionConfig, false);
    this.sqlServerActionConfig = sqlServerActionConfig;
  }

  /**
   * MSSQL Action Config.
   */
  public static class SqlServerActionConfig extends DBSpecificQueryConfig {

    @Name(SqlServerConstants.INSTANCE_NAME)
    @Description(SqlServerConstants.INSTANCE_NAME_DESCRIPTION)
    @Nullable
    public String instanceName;

    @Name(SqlServerConstants.QUERY_TIMEOUT)
    @Description(SqlServerConstants.QUERY_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer queryTimeout = -1;

    @Name(SqlServerConstants.AUTHENTICATION)
    @Description(SqlServerConstants.AUTHENTICATION_DESCRIPTION)
    @Nullable
    public String authenticationType;

    @Name(SqlServerConstants.APPLICATION_INTENT)
    @Description(SqlServerConstants.APPLICATION_INTENT_DESCRIPTION)
    @Nullable
    public String applicationIntent;

    @Name(SqlServerConstants.CONNECT_TIMEOUT)
    @Description(SqlServerConstants.CONNECT_TIMEOUT_DESCRIPTION)
    @Nullable
    public Integer connectTimeout;

    @Name(SqlServerConstants.COLUMN_ENCRYPTION)
    @Description(SqlServerConstants.COLUMN_ENCRYPTION_DESCRIPTION)
    @Nullable
    public Boolean columnEncryption;

    @Name(SqlServerConstants.ENCRYPT)
    @Description(SqlServerConstants.ENCRYPT_DESCRIPTION)
    @Nullable
    public Boolean encrypt;

    @Name(SqlServerConstants.TRUST_SERVER_CERTIFICATE)
    @Description(SqlServerConstants.TRUST_SERVER_CERTIFICATE_DESCRIPTION)
    @Nullable
    public Boolean trustServerCertificate;

    @Name(SqlServerConstants.WORKSTATION_ID)
    @Description(SqlServerConstants.WORKSTATION_ID_DESCRIPTION)
    @Nullable
    public String workstationId;

    @Name(SqlServerConstants.FAILOVER_PARTNER)
    @Description(SqlServerConstants.FAILOVER_PARTNER_DESCRIPTION)
    @Nullable
    public String failoverPartner;

    @Name(SqlServerConstants.PACKET_SIZE)
    @Description(SqlServerConstants.PACKET_SIZE_DESCRIPTION)
    @Nullable
    public Integer packetSize;

    @Name(SqlServerConstants.CURRENT_LANGUAGE)
    @Description(SqlServerConstants.CURRENT_LANGUAGE_DESCRIPTION)
    @Nullable
    public String currentLanguage;

    @Override
    public String getConnectionString() {
      return SqlServerUtil.getConnectionString(host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return SqlServerUtil.composeDbSpecificArgumentsMap(instanceName, authenticationType, applicationIntent,
                                                         connectTimeout, columnEncryption, encrypt,
                                                         trustServerCertificate, workstationId, failoverPartner,
                                                         packetSize, queryTimeout);
    }

    @Override
    public List<String> getInitQueries() {
      if (!Strings.isNullOrEmpty(currentLanguage)) {
        return Collections.singletonList(String.format(SqlServerConstants.SET_LANGUAGE_QUERY_FORMAT, currentLanguage));
      }

      return Collections.emptyList();
    }
  }
}

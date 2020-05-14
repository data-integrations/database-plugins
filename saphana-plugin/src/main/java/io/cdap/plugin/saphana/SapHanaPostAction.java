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

package io.cdap.plugin.saphana;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.db.batch.action.AbstractQueryAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryActionConfig;

/**
 * Represents SAP HANA post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(SapHanaConstants.PLUGIN_NAME)
@Description("Runs a SAP HANA query after a pipeline run.")
public class SapHanaPostAction extends AbstractQueryAction {

  private final SapHanaQueryActionConfig sapHanaQueryActionConfig;

  public SapHanaPostAction(SapHanaQueryActionConfig sapHanaQueryActionConfig) {
    super(sapHanaQueryActionConfig, false);
    this.sapHanaQueryActionConfig = sapHanaQueryActionConfig;
  }

  /**
   * Action specific config overrides
   */
  public static class SapHanaQueryActionConfig extends DBSpecificQueryActionConfig {

    @Override
    public String getConnectionString() {
      return String.format(SapHanaConstants.SAPHANA_CONNECTION_STRING_FORMAT, host, port);
    }

  }
}

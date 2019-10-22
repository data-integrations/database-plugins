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

package io.cdap.plugin.teradata.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.batch.action.AbstractDBAction;
import io.cdap.plugin.teradata.TeradataConstants;

/**
 * Action that runs Teradata command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(TeradataConstants.PLUGIN_NAME)
@Description("Action that runs a Teradata command")
public class TeradataAction extends AbstractDBAction {
  private final TeradataActionConfig config;

  public TeradataAction(TeradataActionConfig config) {
    super(config, false);
    this.config = config;
  }
}
